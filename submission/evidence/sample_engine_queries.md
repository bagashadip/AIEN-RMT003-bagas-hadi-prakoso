# Sample Engine Queries

### 1. What is total net revenue by sales channel?

```sql
SELECT sales_channel, SUM(net_revenue) AS net_revenue FROM gold.order_360 GROUP BY sales_channel ORDER BY net_revenue DESC
```

Result:

| sales_channel | net_revenue |
|---|---|
| WEB | 3,012,386.63 |
| STORE | 2,953,451.26 |
| MOBILE_APP | 2,942,812.81 |
| MARKETPLACE | 2,879,822.61 |

_4 baris._

### 2. Which channels had the highest refund rate?

```sql
SELECT sales_channel, AVG(CASE WHEN refunded_amount > 0 THEN 1.0 ELSE 0.0 END) AS refund_rate FROM gold.order_360 GROUP BY sales_channel ORDER BY refund_rate DESC
```

Result:

| sales_channel | refund_rate |
|---|---|
| MOBILE_APP | 0.1796 |
| WEB | 0.1759 |
| MARKETPLACE | 0.1712 |
| STORE | 0.1681 |

_4 baris._

### 3. What was the daily return rate?

```sql
SELECT metric_date, return_rate FROM gold.executive_kpis_daily ORDER BY metric_date
```

Result:

| metric_date | return_rate |
|---|---|
| 2026-06-22 | 0.1532 |
| 2026-06-23 | 0.1321 |
| 2026-06-24 | 0.1364 |
| 2026-06-25 | 0.1146 |
| 2026-06-26 | 0.1415 |

_90 baris, ditampilkan 5 pertama._

### 4. Which campaigns had the best roas?

```sql
SELECT campaign_id, SUM(net_revenue) / NULLIF(SUM(campaign_spend), 0) AS roas FROM gold.channel_campaign_daily GROUP BY campaign_id HAVING SUM(campaign_spend) > 0 ORDER BY roas DESC
```

Result:

| campaign_id | roas |
|---|---|
| CMP-04 | 41.5469 |
| CMP-05 | 40.195 |
| CMP-09 | 39.9864 |
| CMP-02 | 39.8407 |
| CMP-01 | 38.8945 |

_10 baris, ditampilkan 5 pertama._

### 5. Which product categories generated the most revenue?

```sql
SELECT active_category, SUM(net_revenue) AS net_revenue FROM gold.product_daily GROUP BY active_category ORDER BY net_revenue DESC
```

Result:

| active_category | net_revenue |
|---|---|
| fashion | 2,860,800.7 |
| electronics | 2,739,732.24 |
| sports | 2,627,154.9 |
| home | 1,997,396.29 |
| grocery | 1,873,323.29 |

_6 baris, ditampilkan 5 pertama._

### 6. Which products had stockouts?

```sql
SELECT product_id, metric_date FROM gold.product_daily WHERE stockout_flag IS TRUE ORDER BY metric_date, product_id
```

Result:

| product_id | metric_date |
|---|---|
| PROD-00026 | 2026-06-22 |
| PROD-00028 | 2026-06-22 |
| PROD-00032 | 2026-06-22 |
| PROD-00045 | 2026-06-22 |
| PROD-00048 | 2026-06-22 |

_416 baris, ditampilkan 5 pertama._

### 7. Which orders were fully refunded?

```sql
SELECT order_id, refunded_amount, net_revenue FROM gold.order_360 WHERE refunded_amount > 0 AND net_revenue <= 0 ORDER BY order_id
```

Result:

| order_id | refunded_amount | net_revenue |
|---|---|---|
| ORD-000018 | 1,063.59 | 0 |
| ORD-000058 | 3,203.06 | 0 |
| ORD-000077 | 4,028.54 | 0 |
| ORD-000085 | 880.66 | 0 |
| ORD-000091 | 3,095.43 | 0 |

_458 baris, ditampilkan 5 pertama._

### 8. When was the data last refreshed?

```sql
SELECT MAX(data_freshness_utc) AS last_refresh_utc FROM gold.executive_kpis_daily
```

Result:

| last_refresh_utc |
|---|
| 2026-10-10 00:27:00+00:00 |

_1 baris._
