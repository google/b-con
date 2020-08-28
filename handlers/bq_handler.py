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
"""Handler for BigQuery.

This handler connects with BigQuery and uploads data and runs sql queries.
"""
import datetime
import io
import json

from absl import app
from absl import flags
from absl import logging
from typing import List, Dict, Any

from google.cloud import bigquery
from google.cloud.bigquery import job

import google.cloud.exceptions

from utils import config
from utils import util

FLAGS = flags.FLAGS


def _get_bq_client() -> bigquery.Client:
  # Uses service account if environment variable GOOGLE_APPLICATION_CREDENTIALS
  # is set. This is set through config.yaml.
  return bigquery.Client()


def _fix_keys_in_data(table_data: List[dict],
                      insert_time: datetime.datetime = None) -> List[dict]:
  # Get time of insertion.
  if not insert_time:
    insert_time = util.get_current_time()
  # Fix key names.
  # keys = table_data[0].keys()
  # fixed_keys = [_remove_special_chars(c) for c in keys]
  # key_map = dict(zip(keys, fixed_keys))

  # Contains mapping of column names to fixed column names.
  key_map = dict()

  # Fix key names.
  # Get all possible columns.
  for row in table_data:
    keys = row.keys()
    fixed_keys = [_remove_special_chars(c) for c in keys]
    key_map.update(dict(zip(keys, fixed_keys)))

  table_data_updated = []

  for row in table_data:
    new_row = {key_map[k]: v for k, v in row.items()}
    # Add timestamp.
    new_row['insert_time'] = str(insert_time)
    table_data_updated.append(new_row)

  return table_data_updated


def _remove_special_chars(field: str) -> str:
  return field.replace(' ', '_').replace('(', '').replace(')', '').lower()


def upload_to_bq(table_data: List[dict],
                 table_id: str,
                 insert_time: datetime.datetime = None,
                 delete: bool = False) -> None:
  """Uploads given data to BigQuery.

  This function uploads the data to a table in BigQuery. Table needs to exist,
  but doesn't need to have columns. Any missing columns will be auto created.

  Args:
    table_data: Row data containing List of Dicts.
    table_id: Name of the table.
    insert_time: Time of insertion.
    delete: Delete existing data.
  """
  logging.info('Uploading data to table: %s', table_id)
  if not table_data:
    return

  client = _get_bq_client()
  table_data_updated = _fix_keys_in_data(table_data, insert_time)
  # Add new columns to table if any.
  schema = _add_new_columns(client, table_id, table_data_updated[0].keys())

  # Upload the data.
  # errors = client.insert_rows(table, table_data_updated)

  job_config = job.LoadJobConfig(
      # Create the table if it doesn't exist.
      create_disposition='CREATE_IF_NEEDED',
      source_format='NEWLINE_DELIMITED_JSON',
      write_disposition='WRITE_TRUNCATE' if delete else 'WRITE_APPEND',
      # Schema has to be specified, otherwise client library converts
      # strings to timestamp and integers for no reason.
      schema=schema,
      # Ignore fields that don't have a value. Needed for invoices.
      ignore_unknown_values=True,
  )

  load_job = client.load_table_from_json(
      table_data_updated, table_id, job_config=job_config)

  # Wait for job to complete.
  load_job.result()

  if load_job.errors:
    for e in load_job.errors:
      logging.error(e)

  client.close()
  logging.info('Finished uploading data to table: %s', table_id)


def _delete_rows(client: bigquery.Client, table_id: str) -> None:
  """Delete all rows in a table."""
  query = f'DELETE FROM `{table_id}` WHERE TRUE'
  job = client.query(query)
  try:
    rows = job.result()
  except google.cloud.exceptions.GoogleCloudError as e:
    logging.error(e)
    raise e


def _add_new_columns(client: bigquery.Client, table_id: str,
                     columns: List[str]) -> List[Dict]:
  """Adds any new columns if they are missing.

  Creates new string columns for every column if it doesn't exist.

  Args:
    client: The BigQuery client.
    table_id: Table id.
    columns: List of columns.

  Returns:
    The table schema.
  """
  try:
    table = client.get_table(table_id)
  except google.api_core.exceptions.NotFound:
    logging.error(
        'Table: \'%s\' not found - please create the table. It is okay to create it with no columns.',
        table_id)
    raise
  new_fields = []
  for c in columns:
    field = bigquery.SchemaField(c, 'STRING')
    if field not in table.schema:
      new_fields.append(field)

  if new_fields:
    logging.info('Found new fields: %s', new_fields)
    table.schema += new_fields
    client.update_table(table, ['schema'])

  return table.schema


def run_sql(sql_script_path: str) -> bool:
  """Runs a sql load script and returns job completion status.

  Args:
    sql_script_path: Path to the sql script file.

  Returns:
    True if the job completed successfully, false otherwise.
  """

  client = _get_bq_client()

  with open(sql_script_path, 'r') as f:
    raw_query = f.read()

  query = _populate_table_names(raw_query)

  job = client.query(query)
  try:
    rows = job.result()
  except google.cloud.exceptions.GoogleCloudError as e:
    logging.error(e)
    return list(), job.errors

  return list(rows), job.errors


def _populate_table_names(query: str) -> str:
  tables = config.params['bigquery']['tables']
  for k, v in tables.items():
    search_value = f'$${k}$$'
    query = query.replace(search_value, v)

  return query


if __name__ == '__main__':
  app.run(main)
