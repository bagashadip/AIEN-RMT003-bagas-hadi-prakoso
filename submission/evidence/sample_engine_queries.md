# Sample Engine Queries

### 1. What is total net revenue by sales channel?

```sql
SELECT sales_channel, SUM(net_revenue) AS net_revenue FROM gold.order_360 GROUP BY sales_channel ORDER BY net_revenue DESC
```

### 2. Which channels had the highest refund rate?

```sql
SELECT sales_channel, AVG(CASE WHEN refunded_amount > 0 THEN 1.0 ELSE 0.0 END) AS refund_rate FROM gold.order_360 GROUP BY sales_channel ORDER BY refund_rate DESC
```

### 3. What was the daily return rate?

```sql
SELECT metric_date, return_rate FROM gold.executive_kpis_daily ORDER BY metric_date
```

### 4. Which campaigns had the best roas?

```sql
SELECT campaign_id, SUM(net_revenue) / NULLIF(SUM(campaign_spend), 0) AS roas FROM gold.channel_campaign_daily GROUP BY campaign_id HAVING SUM(campaign_spend) > 0 ORDER BY roas DESC
```

### 5. Which product categories generated the most revenue?

```sql
SELECT active_category, SUM(net_revenue) AS net_revenue FROM gold.product_daily GROUP BY active_category ORDER BY net_revenue DESC
```

### 6. Which products had stockouts?

```sql
SELECT product_id, metric_date FROM gold.product_daily WHERE stockout_flag IS TRUE ORDER BY metric_date, product_id
```

### 7. Which orders were fully refunded?

```sql
SELECT order_id, refunded_amount, net_revenue FROM gold.order_360 WHERE refunded_amount > 0 AND net_revenue <= 0 ORDER BY order_id
```

### 8. When was the data last refreshed?

```sql
SELECT MAX(data_freshness_utc) AS last_refresh_utc FROM gold.executive_kpis_daily
```
