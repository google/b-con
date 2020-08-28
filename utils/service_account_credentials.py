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
"""Get credentials of the service account specified.

This module includes functions which allow to use OAuth credentials of a service
account.
"""
import os

from absl import app
from absl import flags
from absl import logging

from google.oauth2 import service_account
import googleapiclient.discovery

from utils import config

FLAGS = flags.FLAGS

_SCOPES = [
    'https://www.googleapis.com/auth/doubleclickbidmanager',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/display-video-user-management',
    'https://www.googleapis.com/auth/dfareporting',

    # Needed for getting user account profiles.
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/drive',
]


def _get_credentials():
  service_account_file = config.params['google_application_credentials']
  return service_account.Credentials.from_service_account_file(
      service_account_file, scopes=_SCOPES)


def get_dbm_service():
  """Gets the bid manager service."""
  credentials = _get_credentials()
  service = googleapiclient.discovery.build(
      'doubleclickbidmanager',
      'v1',
      credentials=credentials,
      cache_discovery=False,
  )
  return service


def get_dv_service():
  """Gets the displayvideo service.

  The displayvideo API is the new API which will eventually replace the bid
  manager API. It is currently needed for user management.
  """
  credentials = _get_credentials()
  service = googleapiclient.discovery.build(
      'displayvideo',
      'v1',
      credentials=credentials,
      cache_discovery=False,
  )
  return service


def get_cm_service():
  creds = _get_credentials()
  service = googleapiclient.discovery.build(
      'dfareporting', 'v3.4', credentials=creds, cache_discovery=False)
  return service


def get_sheets_service():
  credentials = _get_credentials()
  service = googleapiclient.discovery.build(
      'sheets',
      'v4',
      credentials=credentials,
      cache_discovery=False,
  )
  return service


def get_drive_service():
  credentials = _get_credentials()
  service = googleapiclient.discovery.build(
      'drive',
      'v3',
      credentials=credentials,
      cache_discovery=False,
  )
  return service
