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

CREATE OR REPLACE TABLE `$$dash_invoice_report$$` AS (
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
    SAFE.REGEXP_EXTRACT(campaign, r'[A-Z]{4}[0-9]{4}') AS schedule_number,
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

, invoices_orig AS (
  SELECT
    bill_to,
    invoice_number,
    PARSE_TIMESTAMP('%d %b %Y', invoice_date) AS invoice_date,
    PARSE_TIMESTAMP('%d %b %Y', due_date) AS invoice_due_date,
    billing_id,
    currency,
    SAFE_CAST(REPLACE(invoice_amount,',','') AS FLOAT64) AS invoice_amount,
    product,
    SAFE_CAST(REPLACE(gst_pct,'%','') AS FLOAT64) AS gst_pct,
    SAFE_CAST(gst_val AS FLOAT64) AS gst_val,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time
  FROM `$$invoices$$`
)

, invoice_entries_orig AS (
  SELECT
    order_name,
    purchase_order,
    description,
    quantity,
    uom,
    SAFE_CAST(REPLACE(amount, ',','') AS FLOAT64) AS invoice_entry_amount,
    invoice_number,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time
  FROM `$$invoice_entries$$`
)

, reports AS (
  SELECT
    r.*
  FROM reports_orig AS r
  JOIN (
    SELECT
      advertiser_id,
      campaign_id,
      insertion_order_id,
      line_item_id,
      report_start_date,
      report_end_date,
      MAX(SAFE_CAST (insert_time AS TIMESTAMP)) AS insert_time
    FROM reports_orig
    GROUP BY 1,2,3,4,5,6
  ) AS m
  USING(advertiser_id, campaign_id, insertion_order_id, line_item_id, report_start_date, report_end_date, insert_time)
)

, invoices AS (
  SELECT i.*
  FROM invoices_orig AS i
  JOIN (
    SELECT
      invoice_number,
      invoice_due_date,
      billing_id,
      bill_to,
      currency,
      invoice_amount AS invoice_amount,
      product,
      gst_pct,
      gst_val,
      MAX(insert_time) AS insert_time
    FROM invoices_orig
    GROUP BY 1,2,3,4,5,6,7,8,9
  ) AS m
  USING(invoice_number, insert_time)
)

, invoice_entries AS (
  SELECT
    e.*,
    LOWER(TRIM(description)) AS description_formatted
  FROM invoice_entries_orig AS e
  JOIN (
    SELECT invoice_number, MAX(insert_time) AS insert_time
    FROM invoice_entries_orig
    GROUP BY 1
  ) AS m
  USING(invoice_number, insert_time)
)

, invoice_entries_parsed AS (
  SELECT
    invoice_number,
    REGEXP_EXTRACT(description, 'Partner:(.*?)ID: .*') AS partner,
    REGEXP_EXTRACT(description, 'Partner:.*?ID: ([0-9]*)') AS partner_id,
    REGEXP_EXTRACT(description, 'Advertiser:(.*?)ID: .*') AS advertiser,
    REGEXP_EXTRACT(description, 'Advertiser:.*?ID: ([0-9]*)') AS advertiser_id,
    REGEXP_EXTRACT(description, 'Campaign:(.*?)ID: .*') AS campaign,
    REGEXP_EXTRACT(description, 'Campaign:.*?ID: ([0-9]*)') AS campaign_id,
    REGEXP_EXTRACT(description, 'Insertion order:(.*?)ID: .*') AS insertion_order,
    REGEXP_EXTRACT(description, 'Insertion order:.*?ID: ([0-9]*)') AS insertion_order_id,
    invoice_entry_amount,
    SPLIT(description_formatted, '–')[OFFSET(0)] AS desc_reason,
    description_formatted,
    uom,
    quantity,
  FROM invoice_entries
)

, report1 AS (
SELECT
  partner,
  partner_id,
  advertiser,
  advertiser_id,
  campaign,
  campaign_id,
  insertion_order,
  insertion_order_id,
  report_end_date,
  CASE
    WHEN schedule_number IS NULL THEN 'NA'
    ELSE schedule_number
  END AS schedule_number,
  SUM(media_cost_advertiser_currency + platform_fee_adv_currency + cpm_fee_1_adv_currency + media_fee_2_adv_currency) AS amount,
  SUM(revenue_adv_currency) AS revenue_adv_currency,
  SUM(billable_cost_adv_currency) AS billable_cost_adv_currency
FROM reports AS r
GROUP BY 1,2,3,4,5,6,7,8,9,10
)


, invoice1 AS (
SELECT
    partner,
    partner_id,
    advertiser,
    advertiser_id,
    campaign,
    campaign_id,
    insertion_order,
    insertion_order_id,
    invoice_number,
    invoice_date,
    invoice_due_date,
    billing_id,
    bill_to,
    currency,
    product,
    uom,
    quantity,
    gst_pct,
    invoice_amount,
    gst_val,
    SUM(invoice_entry_amount) as sum_invoice_entry_amount
  FROM invoice_entries_parsed
  JOIN invoices USING(invoice_number)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)

SELECT
  COALESCE(i.partner, r.partner) AS partner,
  COALESCE(i.partner_id, r.partner_id) AS partner_id,
  COALESCE(i.advertiser, r.advertiser) AS advertiser,
  COALESCE(i.advertiser_id, r.advertiser_id) AS advertiser_id,
  COALESCE(i.campaign, r.campaign) AS campaign,
  COALESCE(i.campaign_id, r.campaign_id) AS campaign_id,
  COALESCE(i.insertion_order, r.insertion_order) AS insertion_order,
  COALESCE(i.insertion_order_id, r.insertion_order_id) AS insertion_order_id,
  i.invoice_number,
  i.invoice_amount,
  r.report_end_date,
  i.invoice_date,
  i.invoice_due_date,
  i.currency,
  i.bill_to,
  i.gst_val,
  i.gst_pct,
  i.uom,
  i.quantity,
  i.product,
  r.schedule_number,
  r.amount AS report_amount,
  i.sum_invoice_entry_amount,
  r.amount-i.sum_invoice_entry_amount AS diff,
  r.revenue_adv_currency AS revenue_adv_currency,
  r.billable_cost_adv_currency AS billable_cost_adv_currency
FROM report1 AS r
FULL OUTER JOIN invoice1 AS i
  ON r.advertiser_id = i.advertiser_id
  AND r.campaign_id = i.campaign_id
  AND r.insertion_order_id = i.insertion_order_id
  AND r.report_end_date = i.invoice_date
ORDER BY i.invoice_number
)
