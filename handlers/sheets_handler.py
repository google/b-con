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
"""Handler for managing data in spreadsheets.

This handler allows to fetch data from a given spreadsheet for various purposes.
"""

from absl import app
from absl import flags
from absl import logging

from utils import config
from utils import service_account_credentials

FLAGS = flags.FLAGS


def fetch_column(sheet_id, data_range):
  """Fetch a single column from spreadsheet."""
  service = service_account_credentials.get_sheets_service()
  sheet = service.spreadsheets()
  result = sheet.values().get(
      spreadsheetId=sheet_id, range=data_range).execute()
  values = result.get('values', [])
  data = []
  for row in values:
    data.append(row[0])
  return data


def fetch_data(sheet_id, data_range):
  """Fetch a range from spreadsheet."""
  service = service_account_credentials.get_sheets_service()
  sheet = service.spreadsheets()
  result = sheet.values().get(
      spreadsheetId=sheet_id, range=data_range).execute()
  values = result.get('values', [])

  # First row is the header.
  header = values[0]

  # Create a list of dicts.
  data = [dict(zip(header, row)) for row in values[1:]]

  return data
