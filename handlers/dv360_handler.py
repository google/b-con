# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Handler for DV360.

This handler provides functions which trigger various reports on DV360 and
download and consolidate the data from the reports.
"""

import collections
import concurrent.futures
import copy
import json
import socket

from absl import app
from absl import flags
from absl import logging

from utils import user_account_credentials
from utils import service_account_credentials
from utils import config
from utils import util

from handlers import sheets_handler

FLAGS = flags.FLAGS

_ADV_ID = 'Advertiser ID'
_ADV_TIME_ZONE = 'Advertiser Time Zone'

_MAX_WORKERS = 1

_timezone_query = {
    'kind': 'doubleclickbidmanager#query',
    'metadata': {
        'title': 'b-con timezone report',
        'dataRange': 'LAST_90_DAYS',
        'format': 'CSV',
        'sendNotification': False
    },
    'params': {
        'type': 'TYPE_FEE',
        'groupBys': [
            'FILTER_ADVERTISER',
            'FILTER_ADVERTISER_TIMEZONE',
        ],
        'filters': [{
            'type': 'FILTER_PARTNER',
            'value': '234340'
        }],
        'includeInviteData': True,
        # Need to specify at least one metric for the query to work.
        'metrics': ['METRIC_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS'],
    },
    'schedule': {
        'frequency': 'ONE_TIME'
    },
}

_base_query = {
    'kind': 'doubleclickbidmanager#query',
    'metadata': {
        'title': 'b-con metrics report',
        'dataRange': 'CUSTOM_DATES',
        'format': 'CSV',
        'sendNotification': False
    },
    'params': {
        'type': 'TYPE_FEE',
        'groupBys': [
            'FILTER_PARTNER',
            'FILTER_ADVERTISER',
            'FILTER_MEDIA_PLAN',
            'FILTER_INSERTION_ORDER',
            'FILTER_LINE_ITEM',
        ],
        'filters': [{
            'type': 'FILTER_INSERTION_ORDER',
            'value': '10009356'
        }, {
            'type': 'FILTER_ADVERTISER',
            'value': '3686031'
        }, {
            'type': 'FILTER_INSERTION_ORDER',
            'value': '10179643'
        }, {
            'type': 'FILTER_ADVERTISER',
            'value': '3624237'
        }],
        'metrics': [
            'METRIC_CLICKS',
            'METRIC_IMPRESSIONS',
            'METRIC_MEDIA_COST_ADVERTISER',
            'METRIC_PLATFORM_FEE_ADVERTISER',
            'METRIC_CPM_FEE1_ADVERTISER',
            'METRIC_CPM_FEE2_ADVERTISER',
            'METRIC_CPM_FEE3_ADVERTISER',
            'METRIC_CPM_FEE4_ADVERTISER',
            'METRIC_CPM_FEE5_ADVERTISER',
            'METRIC_MEDIA_FEE1_ADVERTISER',
            'METRIC_MEDIA_FEE2_ADVERTISER',
            'METRIC_MEDIA_FEE3_ADVERTISER',
            'METRIC_MEDIA_FEE4_ADVERTISER',
            'METRIC_MEDIA_FEE5_ADVERTISER',
            'METRIC_DATA_COST_ADVERTISER',
            'METRIC_REVENUE_ADVERTISER',
            'METRIC_BILLABLE_COST_ADVERTISER',
        ],
        'includeInviteData': True
    },
    'schedule': {
        'frequency': 'ONE_TIME'
    },
    'reportDataStartTimeMs': 1561910400000,
    'reportDataEndTimeMs': 1564588800000,
    'timezoneCode': 'Pacific/Auckland',
}


class ReportError(Exception):
  pass


def _get_service():
  socket.setdefaulttimeout(300)  # Getting socket.timeout issues sometimes.
  if config.params['dv360']['use_user_credentials']:
    service = user_account_credentials.get_dbm_service()
  else:
    service = service_account_credentials.get_dbm_service()
  return service


def _get_report_status(service, query_id):
  logging.info('Getting report status')
  response = service.queries().getquery(queryId=query_id).execute()
  is_running = response.get('metadata').get('running')
  report_url = response.get('metadata').get(
      'googleCloudStoragePathForLatestReport')

  query_status = {}

  if (not is_running) and report_url:
    # Query completed successfully.
    logging.info('Report completed: %s', query_id)
    query_status['status'] = 'Completed'
    query_status['url'] = report_url
  elif (not is_running) and not report_url:
    # Query failed.
    query_status['status'] = 'Failed'
  else:
    # Query is still running.
    query_status['status'] = 'Running'

  return query_status


def _get_timezone_query_filters(partners):
  filters = []
  for p in partners:
    partner_filter = {}
    partner_filter['type'] = 'FILTER_PARTNER'
    partner_filter['value'] = p
    filters.append(partner_filter)
  return filters


def _get_query_filters(adv_timezone):
  """Create query filters and group by the invoice date.

  Each report can only have one date range. So if we have multiple invoices with
  different invoice dates, we'll need to run one query for each of them.
  """
  filters = collections.defaultdict(list)
  for adv, timezone in adv_timezone.items():
    advertiser_filter = {}
    advertiser_filter['type'] = 'FILTER_ADVERTISER'
    advertiser_filter['value'] = adv
    filters[timezone].append(advertiser_filter)

  return filters


def create_timezone_report(partners):
  logging.info('Creating timezone report.')

  query = copy.deepcopy(_timezone_query)
  timezone_filters = _get_timezone_query_filters(partners)
  query['params']['filters'] = timezone_filters
  service = _get_service()
  response = service.queries().createquery(body=query).execute()
  logging.info('Created DV360 query: %s', response.get('queryId'))
  query_id = response.get('queryId')

  while True:
    # Wait until the report has completed or failed.
    query_status = _get_report_status(service, query_id)
    if query_status['status'] == 'Completed':
      logging.info('Timezone report completed.')
      break
    elif query_status['status'] == 'Failed':
      raise ReportError

    time.sleep(5)

  logging.info(query_status['url'])
  return query_status['url']


def _create_report(service, report_start_time, report_end_time, cur_timezone,
                   cur_filter):

  query = copy.deepcopy(_base_query)
  query['params']['filters'] = cur_filter
  query['reportDataStartTimeMs'] = report_start_time
  query['reportDataEndTimeMs'] = report_end_time
  logging.info(cur_timezone)
  query['timezoneCode'] = cur_timezone
  logging.info(json.dumps(query))
  response = service.queries().createquery(body=query).execute()
  return response.get('queryId')


def create_reports(timezone_report_data, report_start_time, report_end_time):
  logging.info('Creating reports.')
  valid_rows = util.get_valid_rows(timezone_report_data)
  adv_timezone = {}
  for row in valid_rows:
    adv_timezone[row[_ADV_ID]] = row[_ADV_TIME_ZONE]

  logging.info('Found %d timezones.', len(set(adv_timezone.keys())))

  filters = _get_query_filters(adv_timezone)

  service = _get_service()
  queries = []

  # Get data for last month.
  # last_month = datetime.datetime.now() - relativedelta(months=1)
  # end_date = last_month + relativedelta(day=31)

  with concurrent.futures.ThreadPoolExecutor(
      max_workers=_MAX_WORKERS) as executor:
    wait_for = [
        executor.submit(_create_report, service, report_start_time,
                        report_end_time, cur_timezone, cur_filter)
        for cur_timezone, cur_filter in filters.items()
    ]

    for f in concurrent.futures.as_completed(wait_for):
      queries.append(f.result())

  return queries


def _get_report_status(service, query_id):
  response = service.queries().getquery(queryId=query_id).execute()
  is_running = response.get('metadata').get('running')
  report_url = response.get('metadata').get(
      'googleCloudStoragePathForLatestReport')

  query_status = {}

  if (not is_running) and report_url:
    # Query completed successfully.
    logging.info('Report completed: %s', query_id)
    query_status['status'] = 'Completed'
    query_status['url'] = report_url
  elif (not is_running) and not report_url:
    # Query failed.
    query_status['status'] = 'Failed'
  else:
    # Query is still running.
    query_status['status'] = 'Running'

  return query_status


def _get_reports_status(query_ids):
  service = _get_service()
  status_all = []
  with concurrent.futures.ThreadPoolExecutor(
      max_workers=_MAX_WORKERS) as executor:
    wait_for = [
        executor.submit(_get_report_status, service, query_id)
        for query_id in query_ids
    ]

    for f in concurrent.futures.as_completed(wait_for):
      status_all.append(f.result())

  return status_all


def wait_for_reports_to_complete(query_ids):
  while True:
    # TODO: Change logic to only check the ones that are still running.
    statuses = _get_reports_status(query_ids)
    if all([s['status'] == 'Completed' for s in statuses]):
      break

  return [s['url'] for s in statuses]


def get_user_permissions():
  """Get user assigned roles from DV360."""

  # User permissions have to be obtained only with service accounts. Hence
  # getting the service directly instead of using _get_service().
  # Refer: https://developers.google.com/display-video/api/guides/users/overview.
  service = service_account_credentials.get_dv_service()
  response = service.users().list().execute()
  user_perms = []

  for user in response['users']:
    email = util.hash_single(user['email'])
    assigned_user_roles = user['assignedUserRoles']
    for assigned_role in assigned_user_roles:
      if 'partner' in assigned_role['assignedUserRoleId']:
        entity_type = 'partner'
        entity_id = assigned_role['partnerId']
      else:
        entity_type = 'advertiser'
        entity_id = assigned_role['advertiserId']

      user_role = assigned_role['userRole']
      user_perms.append({
          'email': email,
          'entity_type': entity_type,
          'entity_id': entity_id,
          'user_role': user_role,
      })

  return user_perms


def create_advertiser_report():
  pass


def download_timezone_report():
  pass


def download_advertiser_report():
  pass
