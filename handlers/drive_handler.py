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
"""Handler for accessing files and folders on sharepoint.

This handler includes all the functions needed to access and modify files and
folders on sharepoint.
"""
import glob
import io
import os
import pathlib
import zipfile

from typing import List

from absl import app
from absl import flags
from absl import logging

from googleapiclient import http

from utils import config
from utils import service_account_credentials

FLAGS = flags.FLAGS


def _download_file(service: object, file_id: str, file_name: str,
                   download_path: str):
  """Download files to the specified path."""
  request = service.files().get_media(fileId=file_id)
  fh = io.BytesIO()
  downloader = http.MediaIoBaseDownload(fh, request)
  done = False

  while not done:
    status, done = downloader.next_chunk()

  with open(f'{download_path}/{file_name}', 'wb') as f:
    f.write(fh.getvalue())


def download_invoices(folder_id: str, download_path: str):
  """Downloads invoices from the given drive folder id to the specified path."""
  # Check that the destination folder exists.
  pathlib.Path(download_path).mkdir(parents=True, exist_ok=True)
  service = service_account_credentials.get_drive_service()

  files = service.files().list(
      q=f'(\'{folder_id}\' in parents) and (mimeType != \'application/vnd.google-apps.folder\')',
  ).execute()

  for f in files['files']:
    _download_file(service, f['id'], f['name'], download_path)


def extract_zip_files(download_path: str, extract_path: str):
  """Extracts the zip files of the invoices downloaded."""
  for f in glob.glob(os.path.join(download_path, '*.zip')):
    with zipfile.ZipFile(f) as zf:
      zf.extractall(extract_path)


def get_files(extract_path: str):
  """Fetch the paths to all the files."""
  files = []
  for f in glob.glob(os.path.join(extract_path, '*.csv')):
    files.append(f)

  return files


def delete_downloaded_files(folder_paths: List[str]):
  """Deletes downloaded files."""
  for folder_path in folder_paths:
    for f in glob.glob(os.path.join(folder_path, '*')):
      logging.info('Deleting %s', f)
      pathlib.Path(f).unlink()


def _move_drive_file_to_completed(service: object, file_obj: object,
                                  completed_folder_id: str):
  """Moves the file object to the completed drive folder."""
  file_id = file_obj['id']
  previous_parents = ','.join(file_obj['parents'])
  service.files().update(
      fileId=file_id,
      addParents=completed_folder_id,
      removeParents=previous_parents,
      fields='id, parents',
  ).execute()


def mark_drive_files_completed(folder_id, completed_folder_id):
  """Marks all the files in the folder as completed."""
  logging.info('Marking all drive files completed.')
  service = service_account_credentials.get_drive_service()
  files = service.files().list(
      q=f'(\'{folder_id}\' in parents) and (mimeType != \'application/vnd.google-apps.folder\')',
      fields='files(id, parents)',
  ).execute()

  for f in files['files']:
    _move_drive_file_to_completed(service, f, completed_folder_id)
