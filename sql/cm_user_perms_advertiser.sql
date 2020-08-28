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

CREATE OR REPLACE TABLE `$$cm_user_perms_advertiser$$` AS (

-------------------------------------------------------------------------------
-- Get the advertiser id for users who have access to 'ALL' advertisers.
-------------------------------------------------------------------------------
WITH all_advertisers AS (
  SELECT DISTINCT
    email,
    adv.advertiser_id,
  FROM `$$cm_user_perms$$` AS up
  JOIN `$$cm_advertiser_accounts$$` AS adv
    ON up.account_id = adv.account_id
  WHERE advertiser_status = 'ALL'
)

-------------------------------------------------------------------------------
-- Get the advertiser id for users who have access to 'ASSIGNED' advertisers.
-------------------------------------------------------------------------------
, assigned_advertisers AS (
  SELECT DISTINCT
    email,
    advertiser_id,
  FROM `$$cm_user_perms$$`
  JOIN UNNEST(SPLIT(advertisers, ',')) AS advertiser_id
  WHERE advertiser_status = 'ASSIGNED'
    AND campaign_status = 'ALL'
)

-------------------------------------------------------------------------------
-- Combine both.
-------------------------------------------------------------------------------
SELECT * FROM all_advertisers
UNION ALL
SELECT * FROM assigned_advertisers
);
