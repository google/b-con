-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-------------------------------------------------------------------------------
-- Set this up as a view in BigQuery and use it to filter data to only the
-- rows that the users have access to in data studio.
-------------------------------------------------------------------------------

WITH partner_advertiser AS (
  SELECT DISTINCT
    partner_id,
    advertiser_id,
  FROM `<project-id>.bcon.dash_ui`
)

, user_advertiser AS (
  SELECT
    email,
    entity_id AS advertiser_id
  FROM `<project-id>.bcon.user_perms`
  WHERE entity_type = 'advertiser'
  UNION ALL
  SELECT
    email,
    pa.advertiser_id AS advertiser_id
  FROM `<project-id>.bcon.user_perms` AS u
  JOIN partner_advertiser AS pa
    ON u.entity_id = pa.partner_id
  WHERE entity_type = 'partner'
)

SELECT DISTINCT
  email,
  advertiser_id
FROM user_advertiser
