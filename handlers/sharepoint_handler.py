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
import os
import pathlib
import zipfile

from absl import app
from absl import flags
from absl import logging
from office365.runtime.auth import authentication_context
from office365.sharepoint import client_context

from utils import config

FLAGS = flags.FLAGS


class SharepointAuthError(Exception):
  pass


class SharepointError(Exception):
  pass


def get_sharepoint_context(url):
  sharepoint_user = config.params['sharepoint']['user']
  sharepoint_pass = config.params['sharepoint']['pass']

  context_auth = authentication_context.AuthenticationContext(url)
  try:
    if context_auth.acquire_token_for_user(sharepoint_user, sharepoint_pass):
      context = client_context.ClientContext(url, context_auth)
    else:
      raise SharepointAuthError(ctx_auth.get_last_error())
  except TypeError as e:
    logging.error(
        'Something went wrong. Are the sharepoint user and password correct?')
    raise e

  return context


def get_sharepoint_root_folder(context, root_folder_name):
  list_obj = context.web.lists.get_by_title(root_folder_name)
  root_folder = list_obj.root_folder
  context.load(root_folder)
  context.execute_query()

  return root_folder


def get_sharepoint_folder(context, parent_folder, folder_name):
  all_folders = parent_folder.folders
  context.load(all_folders)
  context.execute_query()

  folder = None

  for f in all_folders:
    if f.properties['Name'] == folder_name:
      folder = f
      break

  if not folder:
    raise SharepointError('Folder not found: %s', folder_name)

  return folder


def get_sharepoint_files(context, folder):
  files = folder.files
  context.load(files)
  context.execute_query()

  return files


def _get_folder_from_path(context, root_folder, path):
  # Split the path by '/' and remove any empty elements to account for trailing
  # '/' in the path.
  path_elements = [t for t in path.split('/') if t]

  cur_folder = root_folder

  for e in path_elements:
    cur_folder = get_sharepoint_folder(context, cur_folder, e)

  return cur_folder


def _get_context_and_invoice_files(sharepoint_config):
  url = sharepoint_config['url']
  root_folder_path = sharepoint_config['root_folder']
  invoice_folder_path = sharepoint_config['invoice_folder']
  context = get_sharepoint_context(url)

  root_folder = get_sharepoint_root_folder(context, root_folder_path)

  invoice_folder = _get_folder_from_path(context, root_folder,
                                         invoice_folder_path)

  invoice_files = get_sharepoint_files(context, invoice_folder)

  return context, invoice_files


def download_invoices(sharepoint_config):
  context, invoice_files = _get_context_and_invoice_files(sharepoint_config)

  for f in invoice_files:
    file_name = f.properties['Name']

    logging.info('Downloading file: %s', file_name)

    download_path = config.params['sharepoint']['download_path']

    # Create folder if it doesn't exist.
    pathlib.Path(download_path).mkdir(parents=True, exist_ok=True)

    file_path = os.path.join(download_path, file_name)
    with open(file_path, 'wb') as output_file:
      output_file.write(f.read())


def move_sharepoint_file_to_completed(context, file_obj, sharepoint_config):
  file_name = file_obj.properties['Name']

  relative_url = sharepoint_config['relative_url']
  root_folder_path = sharepoint_config['root_folder']
  completed_folder_path = sharepoint_config['completed_folder']

  # I don't know why moveto requires relative path. Unfortunately, this is the
  # only reason we have a relative_url in the configuration.
  completed_url = relative_url + root_folder_path + completed_folder_path + file_name

  file_obj.moveto(completed_url, 1)
  context.execute_query()


def extract_zip_files():
  """Extracts the zip files of the invoices downloaded."""
  download_path = config.params['sharepoint']['download_path']
  extract_path = config.params['sharepoint']['extract_path']
  for f in glob.glob(os.path.join(download_path, '*.zip')):
    with zipfile.ZipFile(f) as zf:
      zf.extractall(extract_path)


def get_files():
  """Fetch the paths to all the files."""
  extract_path = config.params['sharepoint']['extract_path']
  files = []
  for f in glob.glob(os.path.join(extract_path, '*.csv')):
    files.append(f)

  return files


def delete_downloaded_files():
  download_path = config.params['sharepoint']['download_path']
  for f in glob.glob(os.path.join(download_path, '*')):
    logging.info('Deleting %s', f)
    pathlib.Path(f).unlink()

  extract_path = config.params['sharepoint']['extract_path']
  for f in glob.glob(os.path.join(extract_path, '*')):
    logging.info('Deleting %s', f)
    pathlib.Path(f).unlink()


def mark_sharepoint_files_completed(sharepoint_config):
  logging.info('Marking all sharepoint files completed.')
  context, invoice_files = _get_context_and_invoice_files(sharepoint_config)

  for f in invoice_files:
    # Uncomment this in production.
    move_sharepoint_file_to_completed(context, f, sharepoint_config)
