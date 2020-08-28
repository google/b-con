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

CREATE OR REPLACE TABLE `$$dash_ui$$` AS (
WITH reports_orig AS (
  SELECT
    partner,
    partner_id,
    advertiser,
    advertiser_id,
    advertiser_status,
    advertiser_integration_code,
    campaign,
    campaign_id,
    insertion_order,
    insertion_order_id,
    insertion_order_status,
    insertion_order_integration_code,
    line_item,
    line_item_id,
    advertiser_currency,
    SAFE_CAST(media_cost_advertiser_currency AS FLOAT64) AS media_cost_advertiser_currency,
    SAFE_CAST(platform_fee_adv_currency AS FLOAT64) AS platform_fee_adv_currency,
    SAFE_CAST(REPLACE(platform_fee_rate,'%','') AS FLOAT64) AS platform_fee_rate,
    SAFE_CAST(cpm_fee_1_adv_currency AS FLOAT64) AS cpm_fee_1_adv_currency,
    SAFE_CAST(cpm_fee_2_adv_currency AS FLOAT64) AS cpm_fee_2_adv_currency,
    SAFE_CAST(cpm_fee_3_adv_currency AS FLOAT64) AS cpm_fee_3_adv_currency,
    SAFE_CAST(cpm_fee_4_adv_currency AS FLOAT64) AS cpm_fee_4_adv_currency,
    SAFE_CAST(cpm_fee_5_adv_currency AS FLOAT64) AS cpm_fee_5_adv_currency,
    SAFE_CAST(media_fee_1_adv_currency AS FLOAT64) AS media_fee_1_adv_currency,
    SAFE_CAST(media_fee_2_adv_currency AS FLOAT64) AS media_fee_2_adv_currency,
    SAFE_CAST(media_fee_3_adv_currency AS FLOAT64) AS media_fee_3_adv_currency,
    SAFE_CAST(media_fee_4_adv_currency AS FLOAT64) AS media_fee_4_adv_currency,
    SAFE_CAST(media_fee_5_adv_currency AS FLOAT64) AS media_fee_5_adv_currency,
    SAFE_CAST(data_fees_adv_currency AS FLOAT64) AS data_fees_adv_currency,
    SAFE_CAST(revenue_adv_currency AS FLOAT64) AS revenue_adv_currency,
    SAFE_CAST(billable_cost_adv_currency AS FLOAT64) AS billable_cost_adv_currency,
    SAFE_CAST(report_start_date AS TIMESTAMP) AS report_start_date,
    SAFE_CAST(report_end_date AS TIMESTAMP) AS report_end_date,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time,
    SAFE_CAST(impressions AS INT64) AS impressions,
    SAFE_CAST(clicks AS INT64) AS clicks,
  FROM `$$reports$$`
)

, reports AS (
  SELECT
    r.*
  FROM reports_orig AS r
  JOIN (
    SELECT
      partner_id,
      advertiser_id,
      campaign_id,
      insertion_order_id,
      line_item_id,
      report_start_date,
      report_end_date,
      MAX(SAFE_CAST (insert_time AS TIMESTAMP)) AS insert_time
    FROM reports_orig
    GROUP BY 1,2,3,4,5,6,7
  ) AS m
  USING(advertiser_id, line_item_id, insertion_order_id, report_start_date, report_end_date, insert_time)
)

-- Do the SAFE_metrics aggregation here.
SELECT
  partner,
  partner_id,
  advertiser,
  advertiser_id,
  campaign,
  campaign_id,
  insertion_order,
  insertion_order_id,
  line_item,
  line_item_id,
  report_start_date,
  report_end_date,
  advertiser_currency AS currency,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(media_cost_advertiser_currency + platform_fee_adv_currency + cpm_fee_1_adv_currency + media_fee_2_adv_currency) AS amount,
  SUM(cpm_fee_1_adv_currency) AS cpm_fee_1,
  SUM(media_fee_1_adv_currency) AS media_fee_1,
  SUM(revenue_adv_currency) AS revenue_adv_currency,
  SUM(billable_cost_adv_currency) AS billable_cost_adv_currency
FROM reports AS r
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
