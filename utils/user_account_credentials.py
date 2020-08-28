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
"""Get credentials of the user triggering the process.

This module includes functions which allow to use OAuth credentials as an end
user.
"""

import os

import httplib2
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
import googleapiclient.discovery

from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS

_SCOPES = [
    'https://www.googleapis.com/auth/doubleclickbidmanager',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/dfareporting',
    # User credentials for DV360 cannot be used for user management. Only service
    # accounts.
    'https://www.googleapis.com/auth/drive',

    # Needed for getting user account profiles.
    'https://www.googleapis.com/auth/dfatrafficking',
]


class DummyFlags:
  pass


dummyflags = DummyFlags()
dummyflags.noauth_local_webserver = True
dummyflags.logging_level = 'DEBUG'


def _get_creds():
  # Don't use default creds.
  #try:
  #  credentials = client.GoogleCredentials.get_application_default()
  #  return credentials.create_scoped(_SCOPES)
  #except client.ApplicationDefaultCredentialsError:
  #  # No application default credentials, continue to try other options.
  #  pass

  client_secrets = os.path.join(os.path.dirname(__file__), 'user_creds.json')
  storage = oauthFile.Storage('tempstore.dat')

  flow = client.flow_from_clientsecrets(
      client_secrets,
      scope=_SCOPES,
      message=tools.message_if_missing(client_secrets))

  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, dummyflags)

  return credentials


def get_dbm_service():
  creds = _get_creds()
  service = googleapiclient.discovery.build(
      'doubleclickbidmanager', 'v1', credentials=creds, cache_discovery=False)
  return service


def get_cm_service():
  creds = _get_creds()
  service = googleapiclient.discovery.build(
      'dfareporting', 'v3.4', credentials=creds, cache_discovery=False)
  return service


def get_sheets_service():
  creds = _get_creds()
  service = googleapiclient.discovery.build(
      'sheets', 'v4', credentials=creds, cache_discovery=False)
  return service


def get_drive_service():
  credential = _get_creds()
  service = googleapiclient.discovery.build(
      'drive',
      'v3',
      credentials=credentials,
      cache_discovery=False,
  )
  return service
