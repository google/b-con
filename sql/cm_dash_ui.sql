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

CREATE OR REPLACE TABLE `$$cm_dash_ui$$` AS (

-----------------------------------------------
-- Cast all the columns to the right data type.
-----------------------------------------------
WITH reports_orig AS (
  SELECT
    advertiser,
    advertiser_id,
    campaign,
    campaign_id,
    SAFE_CAST(report_start_date AS TIMESTAMP) AS report_start_date,
    SAFE_CAST(report_end_date AS TIMESTAMP) AS report_end_date,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time,
    SAFE_CAST(impressions AS INT64) AS impressions,
    SAFE_CAST(clicks AS INT64) AS clicks,
  FROM `$$cm_reports$$`
)

-----------------------------------------------
-- Get the latest reports based on insert_time.
-----------------------------------------------
, reports AS (
  SELECT
    r.*
  FROM reports_orig AS r
  JOIN (
    SELECT
      advertiser_id,
      campaign_id,
      report_start_date,
      report_end_date,
      MAX(SAFE_CAST (insert_time AS TIMESTAMP)) AS insert_time
    FROM reports_orig
    GROUP BY 1,2,3,4
  ) AS m
  USING(advertiser_id, campaign_id, report_start_date, report_end_date, insert_time)
)

-----------------------------------------------
-- Aggregate the values.
-----------------------------------------------
SELECT
  advertiser,
  advertiser_id,
  campaign,
  campaign_id,
  report_start_date,
  report_end_date,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
FROM reports AS r
GROUP BY 1,2,3,4,5,6
)
