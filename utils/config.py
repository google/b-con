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
"""Helper module to read and handle the config.

Reads the config.yaml file and makes the contents available as a dictionary.
"""

import os
import yaml

from absl import app
from absl import flags
from absl import logging

from typing import Dict

FLAGS = flags.FLAGS

params = None


def init():
  # Initialize params.
  global params
  with open('config.yaml') as f:
    params = yaml.load(f, Loader=yaml.Loader)

  # Set environment variable for robot account.
  # Required for Cloud library.
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = params[
      'google_application_credentials']


# Run this when module is imported.
if not params:
  # Initialize only if params is None.
  init()
