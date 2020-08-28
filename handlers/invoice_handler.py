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
"""Handler for processing invoice files.

This handler provides functions which allow to process invoices.
"""

import csv
import enum
import re

from absl import app
from absl import flags
from absl import logging

from typing import List, Tuple, Dict, Any

FLAGS = flags.FLAGS

SUPPORTED_PRODUCTS = [
    'Display and Video 360',
    'Campaign Manager',
    'DoubleClick Campaign Manager',
]


class IncorrectProductError(Exception):
  pass


class ProductNotFoundError(Exception):
  pass


class State(enum.Enum):
  """State machine to decide which part of the invoice we're parsing."""
  READ_HEADER = 1
  READ_PRODUCT = 2
  READ_ENTRIES = 3
  READ_GST = 4


def read(invoice_file_path: str) -> Tuple[Dict, List[Dict]]:
  with open(invoice_file_path) as f:
    data = f.read()

  reader = csv.reader(data.splitlines())

  # Structure of the invoice file:
  # 1. Rows until first blank row are the header entries.
  # 2. Next row is the product row.
  # 3. Rows after that are the entries in the invoice.
  # 4. Lastly is the GST.

  state = State.READ_HEADER

  invoice_header = {}
  invoice_entries = []

  for row in reader:

    # Manage state transition.
    if not row and state == State.READ_HEADER:
      state = State.READ_PRODUCT
      continue

    if not row and state == State.READ_PRODUCT:
      state = State.READ_ENTRIES
      continue

    if not row[0] and state == State.READ_ENTRIES:
      state = State.READ_GST
    # End state transition.

    if state == State.READ_HEADER:
      invoice_header.update(read_header(row))

    elif state == State.READ_PRODUCT:
      if (not row) or (row[0].lower() != 'product'):
        raise ProductNotFoundError('Product row not found')

      invoice_header.update(read_header(row))

      if invoice_header['Product'] not in SUPPORTED_PRODUCTS:
        raise IncorrectProductError(
            f'{invoice_header["Product"]} is not supported.')

    elif state == State.READ_ENTRIES:
      invoice_entries.append(row)

    elif state == State.READ_GST:
      invoice_header.update(read_gst(row))

  try:
    invoice_number = invoice_header['Invoice number']
  except KeyError as e:

    try:
      invoice_number = invoice_header['Credit memo number']
    except KeyError as e:

      try:
        invoice_number = invoice_header['Debit memo number']
      except KeyError as e:
        print(invoice_header)
        raise KeyError('Invoice/Credit/Debit number not found')

  invoice_entries_dict = read_entries(invoice_number, invoice_entries)

  return invoice_header, invoice_entries_dict


def read_header(row: List) -> Dict:
  """Each header entry is a key value pair."""
  return {row[0].replace('\ufeff', ''): row[1]}


def read_gst(row: List) -> Dict:
  """Read the GST row."""
  # Check if there is a GST row.
  if not row[0] and 'gst' in row[2].lower():
    search_str = r'\((.*)\)'
    gst_pct = re.search(search_str, row[2])[1]
    gst_val = row[-1]
    return {'gst_pct': gst_pct, 'gst_val': gst_val}

  elif not row[0] and 'gst' in row[3].lower():
    search_str = r'\((.*)\)'
    gst_pct = re.search(search_str, row[3])[1]
    gst_val = row[-1]
    return {'gst_pct': gst_pct, 'gst_val': gst_val}

  else:
    return {}


def read_entries(invoice_number: str, rows: List[List]) -> List[Dict]:
  header = rows[0]
  entries = rows[1:]
  all_entries = [dict(zip(header, [t.strip() for t in e])) for e in entries]
  for e in all_entries:
    e.update({'Invoice number': invoice_number})

  return all_entries
