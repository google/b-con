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

CREATE OR REPLACE TABLE `$$cm_dash_invoice_report$$` AS (
-----------------------------------------------------------------
-- Cast the reports to the right data types.
-----------------------------------------------------------------
WITH reports_orig AS (
  SELECT
    advertiser,
    advertiser_id,
    campaign,
    campaign_id,
    SAFE.REGEXP_EXTRACT(campaign, r'[A-Z]{4}[0-9]{4}') AS schedule_number,
    SAFE_CAST(report_start_date AS TIMESTAMP) AS report_start_date,
    SAFE_CAST(report_end_date AS TIMESTAMP) AS report_end_date,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time,
    SAFE_CAST(impressions AS INT64) AS impressions,
    SAFE_CAST(clicks AS INT64) AS clicks,
  FROM `$$cm_reports$$`
)

-----------------------------------------------------------------
-- Cast the invoices to the right data types.
-----------------------------------------------------------------
, invoices_orig AS (
  SELECT
    bill_to,
    invoice_number,
    PARSE_TIMESTAMP('%d %b %Y', invoice_date) AS invoice_date,
    PARSE_TIMESTAMP('%d %b %Y', due_date) AS invoice_due_date,
    billing_id,
    currency,
    SAFE_CAST(REPLACE(invoice_amount,',','') AS FLOAT64) AS invoice_amount,
    'Campaign Manager' AS product,
    SAFE_CAST(REPLACE(gst_pct,'%','') AS FLOAT64) AS gst_pct,
    SAFE_CAST(gst_val AS FLOAT64) AS gst_val,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time
  FROM `$$invoices$$`
  WHERE product IN ('Campaign Manager', 'DoubleClick Campaign Manager')
)

-----------------------------------------------------------------
-- Cast the invoice entries to the right data types.
-----------------------------------------------------------------
, invoice_entries_orig AS (
  SELECT
    order_name,
    purchase_order,
    description,
    uom,
    SAFE_CAST(REPLACE(unit_price, ',','') AS FLOAT64) AS unit_price,
    SAFE_CAST(REPLACE(quantity, ',','') AS FLOAT64) AS quantity,
    SAFE_CAST(REPLACE(amount, ',','') AS FLOAT64) AS invoice_entry_amount,
    invoice_number,
    SAFE_CAST(insert_time AS TIMESTAMP) AS insert_time
  FROM `$$invoice_entries$$`
)

-----------------------------------------------------------------
-- Get the latest version of reports using insert_time.
-----------------------------------------------------------------
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
  USING(advertiser_id, campaign_id, insert_time)
)

-----------------------------------------------------------------
-- Get the latest version of invoices using insert_time.
-----------------------------------------------------------------
, invoices AS (
  SELECT
    i.*
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

--------------------------------------------------------------------------
-- Get the latest version of invoice entries using insert_time.
-----------------------------------------------------------------
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

-----------------------------------------------------------------
-- Extract the advertiser and campaign details from description.
-----------------------------------------------------------------
, invoice_entries_parsed AS (
  SELECT
    invoice_number,
    SAFE.REGEXP_EXTRACT(description, 'Advertiser:(.*?),.*') AS advertiser,
    SAFE.REGEXP_EXTRACT(description, 'Advertiser:.*?ID: ([0-9]*)') AS advertiser_id,
    SAFE.REGEXP_EXTRACT(description, 'Campaign:(.*?),.*') AS campaign,
    SAFE.REGEXP_EXTRACT(description, 'Campaign:.*?ID: ([0-9]*)') AS campaign_id,
    SAFE.REGEXP_EXTRACT(description, 'Fee: (.*)') AS fee,
    invoice_entry_amount,
    description_formatted,
    uom,
    quantity,
  FROM invoice_entries
)

-----------------------------------------------------------------
-- Aggregate the report metrics.
-----------------------------------------------------------------
, report1 AS (
SELECT
  advertiser,
  advertiser_id,
  campaign,
  campaign_id,
  report_end_date,
  COALESCE(schedule_number, 'NA') AS schedule_number,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
FROM reports AS r
GROUP BY 1,2,3,4,5,6
)


-----------------------------------------------------------------
-- Aggregate the invoice metrics.
-----------------------------------------------------------------
, invoice1 AS (
SELECT
    advertiser,
    advertiser_id,
    campaign,
    campaign_id,
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
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

-----------------------------------------------------------------
-- Combine UI report with invoice.
-----------------------------------------------------------------
SELECT
  COALESCE(i.advertiser, r.advertiser) AS advertiser,
  COALESCE(i.advertiser_id, r.advertiser_id) AS advertiser_id,
  COALESCE(i.campaign, r.campaign) AS campaign,
  COALESCE(i.campaign_id, r.campaign_id) AS campaign_id,
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
  r.clicks AS report_clicks,
  r.impressions AS report_impressions,
  i.sum_invoice_entry_amount,
FROM report1 AS r
FULL OUTER JOIN invoice1 AS i
  ON r.advertiser_id = i.advertiser_id
  AND r.campaign_id = i.campaign_id
  AND r.report_end_date = i.invoice_date
ORDER BY i.invoice_number
)
