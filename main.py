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
"""The main file where processing begins.

All processing begins in this file. Multiple workflows are available and these
are triggered with the relevant command line arguments.

The following workflows are supported:
  dv360
  cm
  invoice
  sql_load
  admin_users
  user_perms
  cm_user_perms

Sample usage:
  python main.py --workflow=dv360 --month=1 --year=2020
  --ignore_invoice_errors=True

Enable debug messages:
  python main.py --workflow=dv360 --month=1 --year=2020
  --ignore_invoice_errors=True --verbosity=1

It is better to run this with the scripts provided in the scripts folder. This
allows to specify the service account that should be used when accessing various
resources.
"""

import pathlib

from absl import app
from absl import flags
from absl import logging

from handlers import cm_handler
from handlers import drive_handler
from handlers import dv360_handler
from handlers import bq_handler
from handlers import invoice_handler
from handlers import sharepoint_handler
from handlers import sheets_handler

from utils import config
from utils import util

FLAGS = flags.FLAGS

flags.DEFINE_enum('workflow', None, [
    'dv360',
    'cm',
    'invoice',
    'sql_load',
    'admin_users',
    'user_perms',
    'cm_user_perms',
], 'Which workflow to run')
flags.DEFINE_boolean(
    'ignore_invoice_errors', True,
    'Ignore any runtime errors when processing the invoices (Incorrect product, missing columns etc.'
)
flags.DEFINE_integer(
    'month',
    None,
    '(Optional) For DV360 workflow, which month to run for. If not specified, will run for the previous month. Values: 1-12. Mandatory if year is specified.',
)
flags.DEFINE_integer(
    'year',
    None,
    '(Optional) For DV360 workflow, which year to run for. If not specified, will run for the current year. Mandatory if month is specified.',
)
flags.mark_flag_as_required('workflow')


def process_dv360(month=None, year=None):
  """Main function that handles the ETL for DV360 reports to BigQuery."""

  for sheet in config.params['sheets']:
    sheet_id = sheet['sheet_id']
    partners_range = sheet['partners_range']

    partners = sheets_handler.fetch_column(sheet_id, partners_range)

    timezone_report_url = dv360_handler.create_timezone_report(partners)
    data = util.fetch_url(timezone_report_url)

    report_start_time, report_end_time = util.get_dv360_report_times(
        month, year)
    report_start_date, report_end_date = util.get_dv360_report_dates(
        month, year)

    query_ids = dv360_handler.create_reports(data, report_start_time,
                                             report_end_time)
    logging.info(query_ids)
    report_urls = dv360_handler.wait_for_reports_to_complete(query_ids)
    logging.info(report_urls)

    current_time = util.get_current_time()

    all_data = []

    for url in report_urls:
      data = util.fetch_url(url)
      data_valid = util.get_valid_rows(data)
      data_with_dates = util.add_date_columns(data_valid, report_start_date,
                                              report_end_date)

      all_data += data_with_dates

    table_reports = config.params['bigquery']['tables']['reports']
    bq_handler.upload_to_bq(all_data, table_reports, current_time)


def process_cm(month=None, year=None):
  """Main function that handles the ETL for DV360 reports to BigQuery."""
  report_name = config.params['cm']['report_name']
  report_start_date, report_end_date = util.get_dv360_report_dates(month, year)
  accounts = cm_handler.get_accounts()
  all_data = []
  for account in accounts:
    account_id = account['account_id']
    profile_id = account['profile_id']
    logging.info('Processing account: %s using profile: %s', account_id,
                 profile_id)
    report = cm_handler.report_exists(account_id, profile_id, report_name)

    if not report:
      logging.info('Report not found. Creating new report.')
      report = cm_handler.create_report(account_id, profile_id, report_name,
                                        report_start_date, report_end_date)

    else:  # Report exists, check dates.
      cur_start_date = report['criteria']['dateRange']['startDate']
      cur_end_date = report['criteria']['dateRange']['endDate']

      if cur_start_date != report_start_date or cur_end_date != report_end_date:
        report['criteria']['dateRange']['startDate'] = report_start_date
        report['criteria']['dateRange']['endDate'] = report_end_date
        # Update the report.
        logging.info('Updating dates for report.')
        cm_handler.update_report(profile_id, report)

    logging.info('Running report.')
    data = cm_handler.run_report_and_wait(profile_id, report['id'])
    data_with_dates = util.add_date_columns(data, report_start_date,
                                            report_end_date)
    all_data += data_with_dates

  table_id = config.params['bigquery']['tables']['cm_reports']
  current_time = util.get_current_time()
  bq_handler.upload_to_bq(all_data, table_id, current_time)


def _process_invoices_sharepoint():
  sheet_id = config.params['sharepoint']['sheet_id']
  sheet_range = config.params['sharepoint']['range']

  sharepoint_data = sheets_handler.fetch_data(sheet_id, sheet_range)

  for row in sharepoint_data:

    sharepoint_handler.download_invoices(row)
    sharepoint_handler.extract_zip_files()

    invoice_files = sharepoint_handler.get_files()

    all_headers = []
    all_entries = []
    error_invoices = []

    for f in invoice_files:
      try:
        header, entries = invoice_handler.read(f)
      except invoice_handler.ProductNotFoundError as e:
        logging.error('Product: %s , skipping: %s', e, f)
        error_invoices.append(f)
        continue

      except invoice_handler.IncorrectProductError as e:
        logging.error('Incorrect product: %s, skipping: %s', e, f)
        error_invoices.append(f)
        continue

      except KeyError as e:
        logging.error('Key: %s not found in: %s', e, f)
        error_invoices.append(f)
        continue
      except IndexError as e:
        logging.error('Error: %s in: %s', e, f)
        error_invoices.append(f)
        continue

      all_headers.append(header)
      all_entries += entries

    table_invoices = config.params['bigquery']['tables']['invoices']
    table_invoice_entries = config.params['bigquery']['tables'][
        'invoice_entries']
    bq_handler.upload_to_bq(all_headers, table_invoices)
    bq_handler.upload_to_bq(all_entries, table_invoice_entries)

    if FLAGS.ignore_invoice_errors or len(error_invoices) == 0:
      if config.params['sharepoint']['delete_downloaded_files']:
        sharepoint_handler.delete_downloaded_files()

      if config.params['sharepoint']['mark_sharepoint_files_completed']:
        sharepoint_handler.mark_sharepoint_files_completed(row)
    else:
      for i in error_invoices:
        logging.error('Errors with the following invoice: %s', i)


def _process_invoices_drive():
  sheet_id = config.params['drive']['sheet_id']
  sheet_range = config.params['drive']['range']
  drive_data = sheets_handler.fetch_data(sheet_id, sheet_range)
  download_path = config.params['drive']['download_path']
  extract_path = config.params['drive']['extract_path']

  for row in drive_data:

    drive_handler.download_invoices(row['invoice_folder'], download_path)
    drive_handler.extract_zip_files(download_path, extract_path)

    invoice_files = drive_handler.get_files(extract_path)

    all_headers = []
    all_entries = []
    error_invoices = []

    for f in invoice_files:
      try:
        header, entries = invoice_handler.read(f)
      except invoice_handler.ProductNotFoundError as e:
        logging.error('Product: %s , skipping: %s', e, f)
        error_invoices.append(f)
        continue

      except invoice_handler.IncorrectProductError as e:
        logging.error('Incorrect product: %s, skipping: %s', e, f)
        error_invoices.append(f)
        continue

      except KeyError as e:
        logging.error('Key: %s not found in: %s', e, f)
        error_invoices.append(f)
        continue
      except IndexError as e:
        logging.error('Error: %s in: %s', e, f)
        error_invoices.append(f)
        continue

      all_headers.append(header)
      all_entries += entries

    table_invoices = config.params['bigquery']['tables']['invoices']
    table_invoice_entries = config.params['bigquery']['tables'][
        'invoice_entries']
    bq_handler.upload_to_bq(all_headers, table_invoices)
    bq_handler.upload_to_bq(all_entries, table_invoice_entries)

    if FLAGS.ignore_invoice_errors or len(error_invoices) == 0:
      if config.params['drive']['delete_downloaded_files']:
        drive_handler.delete_downloaded_files([download_path, extract_path])

      if config.params['drive']['mark_drive_files_completed']:
        drive_handler.mark_drive_files_completed(row['invoice_folder'],
                                                 row['completed_folder'])
    else:
      for i in error_invoices:
        logging.error('Errors with the following invoice: %s', i)


def process_invoices():
  """Main function that handles the ETL for invoices to BigQuery."""

  invoice_storage = config.params['invoice_storage']

  if invoice_storage == 'drive':
    _process_invoices_drive()
  elif invoice_storage == 'sharepoint':
    _process_invoices_sharepoint()
  else:
    raise 'Invalid storage handler specified in config.yaml.'


def process_load_jobs():
  sql_scripts = [
      'sql/dash_invoice_report.sql',
      'sql/dash_ui.sql',
      'sql/cm_dash_ui.sql',
      'sql/cm_dash_invoice_report.sql',
      'sql/cm_user_perms_advertiser.sql',
  ]

  for script in sql_scripts:
    logging.info('Running %s', script)
    rows, errors = bq_handler.run_sql(script)
    if errors:
      logging.error(errors)


def process_admin_users():
  """Load admin users from config sheet to bigquery."""
  admin_users_dict = []
  for sheet in config.params['sheets']:
    sheet_id = sheet['sheet_id']
    admin_users_range = sheet['admin_users_range']
    partners_range = sheet['partners_range']

    admin_users = [
        a.lower()
        for a in sheets_handler.fetch_column(sheet_id, admin_users_range)
    ]

    partners = sheets_handler.fetch_column(sheet_id, partners_range)
    admin_users_hashed = util.hash(admin_users)

    for p in partners:
      admin_users_dict += [{
          'user': v,
          'partner': p
      } for v in admin_users_hashed]

  table_admin_users = config.params['bigquery']['tables']['admin_users']

  bq_handler.upload_to_bq(
      admin_users_dict, table_admin_users, util.get_current_time(), delete=True)


def process_user_perms():
  """Load user permissions for partners and advertisers into bigquery."""
  user_perms = dv360_handler.get_user_permissions()
  table_user_perms = config.params['bigquery']['tables']['user_perms']
  bq_handler.upload_to_bq(
      user_perms, table_user_perms, util.get_current_time(), delete=True)


def process_cm_user_perms():
  """Load user permissions for CM into bigquery."""
  user_profile_id = cm_handler.get_user_profile_id()
  user_perms = cm_handler.get_user_permissions(profile_id=user_profile_id)
  table_user_perms = config.params['bigquery']['tables']['cm_user_perms']
  current_time = util.get_current_time()
  bq_handler.upload_to_bq(
      user_perms, table_user_perms, current_time, delete=True)

  # Get the accounts/networks advertisers belong to. This is needed to identify
  # advertiser ids for users who have access to 'ALL' advertisers.
  advertiser_accounts = cm_handler.get_advertiser_accounts(user_profile_id)
  table_advertiser_accounts = config.params['bigquery']['tables'][
      'cm_advertiser_accounts']
  bq_handler.upload_to_bq(
      advertiser_accounts, table_advertiser_accounts, current_time, delete=True)


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  # Create log directory if it doesn't exist.
  log_dir = config.params['log_dir']
  pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
  logging.get_absl_handler().start_logging_to_file(log_dir=log_dir)

  workflow = FLAGS.workflow

  month = FLAGS.month
  year = FLAGS.year

  if (month and not year) or (year and not month):
    raise app.UsageError(
        'Either both or neither of month and year should be provided.')

  if workflow == 'dv360':
    process_dv360(month, year)
  elif workflow == 'cm':
    process_cm(month, year)
  elif workflow == 'invoice':
    process_invoices()
  elif workflow == 'sql_load':
    process_load_jobs()
  elif workflow == 'admin_users':
    process_admin_users()
  elif workflow == 'user_perms':
    process_user_perms()
  elif workflow == 'cm_user_perms':
    process_cm_user_perms()


if __name__ == '__main__':
  app.run(main)
