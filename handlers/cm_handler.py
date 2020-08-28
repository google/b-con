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
import csv
import io
import json
import pprint
import random
import socket
import time

from typing import List, Dict

from absl import app
from absl import flags
from absl import logging
from googleapiclient import http

from utils import user_account_credentials
from utils import service_account_credentials
from utils import config
from utils import util

from handlers import sheets_handler

FLAGS = flags.FLAGS

_ADV_ID = 'Advertiser ID'
_ADV_TIME_ZONE = 'Advertiser Time Zone'

_MAX_WORKERS = 1

_REPORT = {
    'name': 'bcon test report',
    'type': 'STANDARD',
    'accountId': 0,
    'criteria': {
        'dateRange': {
            'startDate': '2020-07-01',
            'endDate': '2020-07-31'
        },
        'dimensions': [{
            'name': 'dfa:advertiser'
        }, {
            'name': 'dfa:advertiserId'
        }, {
            'name': 'dfa:campaign'
        }, {
            'name': 'dfa:campaignId'
        }],
        'metricNames': ['dfa:impressions', 'dfa:clicks']
    },
    'format': 'CSV'
}

# The following values control retry behavior while the report is processing.
# Minimum amount of time between polling requests. Defaults to 10 seconds.
MIN_RETRY_INTERVAL = 10
# Maximum amount of time between polling requests. Defaults to 10 minutes.
MAX_RETRY_INTERVAL = 10 * 60
# Maximum amount of time to spend polling. Defaults to 1 hour.
MAX_RETRY_ELAPSED_TIME = 60 * 60
# Chunk size to use when downloading report files. Defaults to 32MB.
CHUNK_SIZE = 32 * 1024 * 1024


class ReportError(Exception):
  pass


class ReportRunDeadlineExceeded(Exception):
  pass


class ReportRunError(Exception):
  pass


def _get_service() -> object:
  socket.setdefaulttimeout(300)  # Getting socket.timeout issues sometimes.
  if config.params['cm']['use_user_credentials']:
    service = user_account_credentials.get_cm_service()
  else:
    service = service_account_credentials.get_cm_service()
  return service


def get_accounts() -> List[dict]:
  """Fetches the account id and the first profile id that has access to it."""
  service = _get_service()
  request = service.userProfiles().list()
  response = request.execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(response))
  accounts = []
  account_details = []
  for i in response['items']:
    if i['accountId'] not in accounts:
      accounts.append(i['accountId'])
      account_details.append({
          'account_id': i['accountId'],
          'profile_id': i['profileId'],
      })
  return account_details


def report_exists(account_id: str, profile_id: str, report_name: str) -> object:
  """Check if a B-Con report already exists.

  Looks for a report by the report name.

  Args:
    account_id: The account/network id.
    profile_id: The profile id of the user owning the report.
    report_name: The name of the report to search for.

  Returns:
    The report object if it exists. None if it doesn't exist.
  """
  service = _get_service()
  request = service.reports().list(profileId=profile_id)
  response = request.execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(response))
  for report in response['items']:
    if report['name'] == report_name:
      logging.info('Found report id: %s with report name: %s', report['id'],
                   report_name)
      return report
  return None


def create_report(account_id: str, profile_id: str, report_name: str,
                  start_date: str, end_date: str) -> object:
  """Create a B-Con report if doesn't exist.

  Creates a campaign manager report with the given name.

  Args:
    account_id: The account/network id.
    profile_id: The profile id of the user owning the report.
    report_name: The name of the report to search for.

  Returns:
    The report object of the newly created report.
  """

  report = copy.deepcopy(_REPORT)
  report['accountId'] = account_id
  report['name'] = report_name
  report['criteria']['dateRange']['startDate'] = start_date
  report['criteria']['dateRange']['endDate'] = end_date

  service = _get_service()
  response = service.reports().insert(
      profileId=profile_id, body=report).execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(response))
  logging.info('Created report with id: %s for report name: %s', response['id'],
               report_name)
  return response


def update_report(profile_id: str, report: dict):
  """Update existing report.

  Updates an existing report.

  Args:
    profile_id: The profile id of the user owning the report.
    report: The report object.

  Returns:
    The report object of the updated report.
  """
  service = _get_service()
  response = service.reports().update(
      profileId=profile_id,
      reportId=report['id'],
      body=report,
  ).execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(response))
  logging.info('Updated report with id: %s for report name: %s', response['id'],
               report['name'])
  return response


def run_report_and_wait(profile_id: str, report_id: str) -> List:
  """Run the report and wait for it to complete and return the data.

  Runs the report and waits for it to complete and returns the data.

  Args:
    profile_id: The profile id of the account being used.
    report_id: The id of the report to run.

  Returns:
    The report data.
  """
  service = _get_service()
  report_file = service.reports().run(
      profileId=profile_id,
      reportId=report_id,
  ).execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(report_file))

  # Wait for report file to finish processing.
  # An exponential backoff strategy is used to conserve request quota.
  sleep = 0
  start_time = time.time()
  while True:
    report_file = service.files().get(
        reportId=report_id, fileId=report_file['id']).execute()

    status = report_file['status']
    if status == 'REPORT_AVAILABLE':
      logging.info('File status is %s, ready to download.', status)
      break
    elif status != 'PROCESSING':
      logging.info('File status is %s, processing failed.', status)
      raise ReportRunError
    elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
      logging.info('File processing deadline exceeded.')
      raise ReportRunDeadlineExceeded

    sleep = _next_sleep_interval(sleep)
    logging.info('File status is %s, sleeping for %d seconds.', status, sleep)
    time.sleep(sleep)

  bytesio = io.BytesIO()
  # Create a get request.
  request = service.files().get_media(
      reportId=report_id, fileId=report_file['id'])

  # Create a media downloader instance.
  # Optional: adjust the chunk size used when downloading the file.
  downloader = http.MediaIoBaseDownload(bytesio, request, chunksize=CHUNK_SIZE)

  # Execute the get request and download the file.
  download_finished = False
  while download_finished is False:
    _, download_finished = downloader.next_chunk()

  csvreader = csv.reader(bytesio.getvalue().decode('utf-8').splitlines())
  report_data = list(csvreader)
  report_data_cleaned = _clean_up(report_data)
  heading = report_data_cleaned[0]
  report_data_objects = []
  for row in report_data_cleaned[1:]:
    report_data_objects.append({h: r for h, r in zip(heading, row)})
  return report_data_objects


def _clean_up(report_data: List[List]) -> List[List]:
  for i, row in enumerate(report_data):
    if row and row[0] == 'Report Fields':
      start_idx = i + 1
      break

  # Last row is total, ignore it.
  return report_data[start_idx:-1]


def delete_report(profile_id: str, report_id: int):
  """Deletes an existing report."""
  service = _get_service()
  response = service.reports().delete(
      profileId=profile_id, reportId=report_id).execute()
  logging.info('Deleted report with id: %s', report_id)


def _next_sleep_interval(previous_sleep_interval):
  """Calculates the next sleep interval based on the previous."""
  min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
  max_interval = previous_sleep_interval * 3 or MIN_RETRY_INTERVAL
  return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))


def get_user_profile_id() -> str:
  service = _get_service()
  response = service.userProfiles().list().execute()
  if logging.get_verbosity() == 1:  # Debug.
    pp = pprint.PrettyPrinter(indent=2)
    logging.debug(pp.pformat(response))

  return response['items'][0]['profileId']


def get_user_permissions(profile_id: str) -> List[Dict]:
  """Get user assigned roles from DV360."""
  service = _get_service()
  page_token = None
  user_profiles = []

  # We need to use page token because max is 1000 results only.
  while True:
    response = service.accountUserProfiles().list(
        profileId=profile_id,
        pageToken=page_token,
    ).execute()
    account_user_profiles = response['accountUserProfiles']
    for i, profile in enumerate(account_user_profiles):
      # If debug enabled, show the first 3 entries.
      if i < 3:
        if logging.get_verbosity() == 1:  # Debug.
          logging.debug('Debugging enabled, showing first 3 entries.')
          pp = pprint.PrettyPrinter(indent=2)
          logging.debug(pp.pformat(profile))

      user_profiles.append({
          'account_id':
              profile.get('accountId', 'NA'),
          'subaccount_id':
              profile.get('subaccountId', 'NA'),
          'email':
              util.hash_single(profile['email']),
          'advertisers':
              ','.join(profile['advertiserFilter'].get('objectIds', [])),
          'advertiser_status':
              profile['advertiserFilter']['status'],
          'campaigns':
              ','.join(profile['campaignFilter'].get('objectIds', [])),
          'campaign_status':
              profile['campaignFilter']['status'],
      })

    page_token = response.get('nextPageToken')
    if not page_token:
      break

  return user_profiles


def get_advertiser_accounts(profile_id: str) -> List[Dict]:
  """Retrieve the account/network that each advertiser belongs to."""
  service = _get_service()
  page_token = None
  advertiser_accounts = []
  while True:
    response = service.advertisers().list(
        profileId=profile_id,
        pageToken=page_token,
    ).execute()

    advertisers = response['advertisers']

    for adv in advertisers:
      advertiser_accounts.append({
          'advertiser_id': adv['id'],
          'account_id': adv['accountId'],
      })

    page_token = response.get('nextPageToken')
    if not page_token:
      break

  return advertiser_accounts


# Not used, leaving here as reference.
def get_campaign_heirarchy(profile_id: str) -> List[Dict]:
  """Retrieve the campaign heirarchy."""
  service = _get_service()
  page_token = None
  campaign_heirarchy = []
  while True:
    response = service.campaigns().list(
        profileId=profile_id,
        pageToken=page_token,
    ).execute()

    campaigns = response['campaigns']

    for camp in campaigns:
      campaign_heirarchy.append({
          'campaign_id': camp['id'],
          'advertiser_id': camp['advertiserId'],
          'account_id': camp['accountId'],
      })

    page_token = response.get('nextPageToken')
    if not page_token:
      break

  return campaign_heirarchy
