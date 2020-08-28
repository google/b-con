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
"""Utility file.

Collection of various utility functions used frequently throughout the code.
"""

import csv
import datetime
import time
import yaml
import base64

from absl import app
from absl import flags
from absl import logging
from urllib import request
from dateutil.relativedelta import relativedelta

from Crypto.Hash import SHA256

from typing import List, Tuple, Dict, Any

FLAGS = flags.FLAGS


def fetch_url(url):
  """Fetch and return a url."""
  with request.urlopen(url) as data:
    data_string = data.read().decode('utf-8')

  return data_string


def get_valid_rows(report_data):
  """Fetch all the rows in the report until it hits a blank in first column.

  This is needed because sometimes DV360 inserts a weird additional metric at
  the end of all the rows. This prevents us from just counting backwards to get
  all the rows.

  Args:
    report_data: The raw report data from DV360.

  Returns:
    List of dict objects with all the valid rows.
  """
  report_data_lines = report_data.splitlines()
  csv_reader = csv.reader(report_data_lines)
  rows = []

  for row in csv_reader:
    # Stop when the first column is blank.
    if not row or not row[0]:
      break

    rows.append(row)

  header = rows[0]
  valid_rows = []
  # Convert to valid dict objects.
  for row in rows[1:]:
    obj = {k: v for k, v in zip(header, row)}
    valid_rows.append(obj)

  return valid_rows


def get_current_time():
  return datetime.datetime.now(datetime.timezone.utc)


def _get_valid_start_date(month: int, year: int) -> datetime.datetime:
  """Returns a valid start date given month and year values.

  Gets the previous month and year if either month or year are none. This is
  because if the code runs on the first of the month, it should get the previous
  completed month's data.

  Args:
    month: Month number.
    year: Year number.

  Returns:
    Valid start date as a datetime object.
  """
  if not year or not month:
    logging.info(
        'Year and/or month not provided. Using previous month and year.')
    cur_time = datetime.datetime.now()
    start_date = datetime.datetime(cur_time.year, cur_time.month - 1, 1)
  else:
    start_date = datetime.datetime(year, month, 1)

  return start_date


def get_dv360_report_times(month: int, year: int) -> Tuple[int, int]:
  start_date = _get_valid_start_date(month, year)
  end_date = start_date + relativedelta(day=31)

  report_start_time = int(time.mktime(start_date.timetuple()) * 1000)
  report_end_time = int(time.mktime(end_date.timetuple()) * 1000)

  return report_start_time, report_end_time


def get_dv360_report_dates(month: int, year: int) -> Tuple[str, str]:
  start_date = _get_valid_start_date(month, year)
  end_date = start_date + relativedelta(day=31)
  return str(start_date.date()), str(end_date.date())


def add_date_columns(data: List[Dict], report_start_date: str,
                     report_end_date: str):
  data_with_dates = []
  for row in data:
    row['report_start_date'] = report_start_date
    row['report_end_date'] = report_end_date
    data_with_dates.append(row)
  return data_with_dates


def hash(data: List) -> List:
  return [
      base64.b64encode(
          SHA256.new(data=t.lower().encode('utf-8')).digest()).decode('utf-8')
      for t in data
  ]


def hash_single(data: str) -> str:
  return base64.b64encode(
      SHA256.new(data=data.lower().encode('utf-8')).digest()).decode('utf-8')
