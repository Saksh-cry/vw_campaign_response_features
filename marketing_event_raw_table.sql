 CREATE TABLE marketing_event_raw_table (
     event_id        VARCHAR(20)   NOT NULL,
    customer_email     VARCHAR(100)   NOT NULL,
    campaign_name     VARCHAR(100)      NOT NULL,
    event_type        VARCHAR(20)   NOT NULL,  -- web / android_app / ios_app
    event_time        DATETIME      NOT NULL,
    channel           VARCHAR(20)   NOT NULL ,
   PRIMARY KEY (event_id)
   );
 alter table marketing_event_raw_table
 ADD column filename  VARCHAR(50) ;
 alter table marketing_event_raw_table
 ADD column   user_name    VARCHAR(200);
 alter table marketing_event_raw_table
 ADD column  rownumber DATETIME;
 alter table marketing_event_raw_table
 ADD column  last_updated_at integer;
  
  
select * from marketing_event_raw_table;
SELECT * 
FROM marketing_event_raw_table 
WHERE event_time IS NULL 
   OR TRY_CAST(event_time as DATETIME) IS NULL;
   OR event_type NOT IN ('open','click','bounce','unsubscribe');

CREATE VIEW marketing_events_clean AS
SELECT * 
FROM marketing_event_raw_table 
WHERE customer_email IS NOT NULL 
  AND TRY_CAST(event_time AS DATETIME) IS NOT NULL 
  AND event_type IN ('open', 'click', 'bounce', 'unsubscribe');

SELECT 
  campaign_name,
  COUNT(*) AS total_events,
  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS click_percentage,
  COUNT(DISTINCT CASE WHEN event_type = 'click' THEN customer_email END) AS unique_clickers
FROM marketing_event_raw_table
GROUP BY campaign_name;


 

DROP VIEW IF EXISTS vw_campaign_events;

CREATE VIEW vw_campaign_events AS
SELECT 
    campaign_name,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT customer_email) AS unique_customers
FROM marketing_event_raw_table
GROUP BY 
    campaign_name,
    event_type;



DROP VIEW IF EXISTS vw_churn_training;
CREATE VIEW vw_churn_training AS
WITH
    -- Set snapshot date
    params AS (
        SELECT
            DATE('2025-07-01') AS snapshot_date,
            DATE('2025-07-01', '+60 days') AS churn_horizon_end
    ),
    -- Orders BEFORE snapshot (features)
    order_hist AS (
        SELECT
            o.customer_id,
            COUNT(DISTINCT o.order_id) AS hist_total_orders,
            COALESCE(SUM(o.net_amount), 0) AS hist_total_revenue,
            AVG(o.net_amount) AS hist_avg_order_value,
            MIN(o.order_date) AS hist_first_order_date,
            MAX(o.order_date) AS hist_last_order_date
        FROM raw_ecommerce_orders_raw_table o
        JOIN params p ON o.order_date < p.snapshot_date
        GROUP BY o.customer_id
    ),
    -- Orders AFTER snapshot (label)
    order_future AS (
        SELECT
            o.customer_id,
            COUNT(DISTINCT o.order_id) AS future_orders
        FROM raw_ecommerce_orders_raw_table o
        JOIN params p ON o.order_date >= p.snapshot_date AND o.order_date < p.churn_horizon_end
        GROUP BY o.customer_id
    ),
    -- Marketing history BEFORE snapshot
    marketing_hist AS (
        SELECT
            me.customer_email,
            COUNT(*) AS mh_total_events,
            SUM(CASE WHEN LOWER(me.event_type) = 'open' THEN 1 ELSE 0 END) AS mh_opens,
            SUM(CASE WHEN LOWER(me.event_type) = 'click' THEN 1 ELSE 0 END) AS mh_clicks,
            SUM(CASE WHEN LOWER(me.event_type) = 'bounce' THEN 1 ELSE 0 END) AS mh_bounces,
            SUM(CASE WHEN LOWER(me.event_type) = 'unsubscribe' THEN 1 ELSE 0 END) AS mh_unsubscribes,
            MAX(me.event_time) AS mh_last_event_time
        FROM marketing_event_raw_table me
        JOIN params p ON me.event_time < p.snapshot_date
        GROUP BY me.customer_email
    ),
    -- Support tickets BEFORE snapshot
    ticket_hist AS (
        SELECT
            st.customer_email,
            COUNT(*) AS th_total_tickets,
            SUM(CASE WHEN st.status = 'Open' THEN 1 ELSE 0 END) AS th_open_tickets,
            SUM(CASE WHEN st.status = 'Resolved' THEN 1 ELSE 0 END) AS th_resolved_tickets,
            SUM(CASE WHEN st.status = 'Closed' THEN 1 ELSE 0 END) AS th_closed_tickets,
            MAX(st.created_at) AS th_last_ticket_time
        FROM raw_support_tickets_raw_table st
        JOIN params p ON st.created_at < p.snapshot_date
        GROUP BY st.customer_email
    )
SELECT
    c.crm_customer_id AS customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.gender,
    c.city,
    c.state,
    c.signup_date,
    c.loyalty_tier,
    c.preferred_channel,
    -- Orders history
    COALESCE(oh.hist_total_orders, 0) AS hist_total_orders,
    COALESCE(oh.hist_total_revenue, 0) AS hist_total_revenue,
    oh.hist_avg_order_value,
    oh.hist_first_order_date,
    oh.hist_last_order_date,
    CASE
        WHEN oh.hist_last_order_date IS NULL THEN NULL
        ELSE CAST((JULIANDAY((SELECT snapshot_date FROM params)) - JULIANDAY(oh.hist_last_order_date)) AS INTEGER)
    END AS hist_recency_days,
    -- Marketing
    COALESCE(mh.mh_total_events, 0) AS mh_total_events,
    COALESCE(mh.mh_opens, 0) AS mh_opens,
    COALESCE(mh.mh_clicks, 0) AS mh_clicks,
    COALESCE(mh.mh_bounces, 0) AS mh_bounces,
    COALESCE(mh.mh_unsubscribes, 0) AS mh_unsubscribes,
    CASE
        WHEN mh.mh_total_events > 0 THEN ROUND(1.0 * mh.mh_clicks / mh.mh_total_events, 4)
        ELSE NULL
    END AS mh_click_rate,
    -- Support
    COALESCE(th.th_total_tickets, 0) AS th_total_tickets,
    COALESCE(th.th_open_tickets, 0) AS th_open_tickets,
    COALESCE(th.th_resolved_tickets, 0) AS th_resolved_tickets,
    COALESCE(th.th_closed_tickets, 0) AS th_closed_tickets,
    -- FINAL LABEL: churn in next 60 days
    CASE
        WHEN ofu.future_orders IS NULL OR ofu.future_orders = 0 THEN 1
        ELSE 0
    END AS churn_label
FROM crm_customer_raw_table c
JOIN params p ON c.signup_date < p.snapshot_date
LEFT JOIN order_hist oh ON oh.customer_id = c.crm_customer_id
LEFT JOIN marketing_hist mh ON mh.customer_email = c.email
LEFT JOIN ticket_hist th ON th.customer_email = c.email
LEFT JOIN order_future ofu ON ofu.customer_id = c.crm_customer_id;



select* from vw_churn_training;









DROP VIEW IF EXISTS vw_next_purchase_features;
CREATE VIEW vw_next_purchase_features AS
WITH
-- 1) Last 3 order amounts per customer
last3 AS (
    SELECT
        customer_id,
        GROUP_CONCAT(net_amount) AS last_3_order_values
    FROM (
        SELECT 
            customer_id,
            net_amount
        FROM raw_ecommerce_orders_raw_table
        ORDER BY order_date DESC
        LIMIT 3
    )
    GROUP BY customer_id
),
-- 2) Order frequency (total orders)
freq AS (
    SELECT
        customer_id,
        COUNT(*) AS order_frequency
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
),
-- 3) Discount history
discount_hist AS (
    SELECT
        customer_id,
        AVG(discount_pct) AS avg_discount_pct
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
),
-- 4) Category affinity (most ordered category)
cat_pref AS (
    SELECT customer_id, category
    FROM (
        SELECT
            customer_id,
            category,
            COUNT(*) AS cnt
        FROM raw_ecommerce_orders_raw_table
        GROUP BY customer_id, category
        ORDER BY cnt DESC
    )
    GROUP BY customer_id
),
-- 5) Next order amount (label)
next_order AS (
    SELECT 
        customer_id,
        order_id,
        LEAD(net_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
        ) AS next_order_amount
    FROM raw_ecommerce_orders_raw_table
)
-- Final dataset
SELECT
    o.customer_id,
    l3.last_3_order_values,
    f.order_frequency,
    d.avg_discount_pct,
    cp.category AS category_affinity,
    n.next_order_amount AS label_next_purchase_amount
FROM raw_ecommerce_orders_raw_table o
LEFT JOIN last3 l3 ON o.customer_id = l3.customer_id
LEFT JOIN freq f ON o.customer_id = f.customer_id
LEFT JOIN discount_hist d ON o.customer_id = d.customer_id
LEFT JOIN cat_pref cp ON o.customer_id = cp.customer_id
LEFT JOIN next_order n ON o.order_id = n.order_id
GROUP BY o.customer_id;




DROP VIEW IF EXISTS vw_campaign_response;
CREATE VIEW view_campaign_response AS
WITH
-- 1) Marketing Engagement from marketing_event_raw_table
eng AS (
    SELECT 
        me.customer_email,
        COUNT(*) AS total_events,
        MIN(event_time) AS first_event,
        MAX(event_time) AS last_event
    FROM marketing_event_raw_table me
    GROUP BY me.customer_email
),
eng_time AS (
    SELECT
        customer_email,
        total_events,
        CAST(julianday('now') - julianday(last_event) AS INT) AS days_since_last_event
    FROM eng
),
-- 2) Ecommerce order metrics
order_val AS (
    SELECT
        o.customer_id,
        AVG(o.net_amount) AS avg_order_value,
        COUNT(*) AS total_orders
    FROM raw_ecommerce_orders_raw_table o
    GROUP BY o.customer_id
),
-- 3) Category affinity from ecommerce
cat_pref AS (
    SELECT customer_id, category
    FROM (
        SELECT 
            customer_id,
            category,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
        FROM raw_ecommerce_orders_raw_table
        GROUP BY customer_id, category
    )
    WHERE rn = 1
),
-- 4) Support tickets
support AS (
    SELECT 
        customer_email,
        COUNT(*) AS total_tickets
    FROM raw_support_tickets_raw_table
    GROUP BY customer_email
)
SELECT
    c.crm_customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.city,
    c.state,
    c.loyalty_tier,
    -- Engagement
    e.total_events AS marketing_events,
    e.days_since_last_event,
    -- Orders
    ov.avg_order_value,
    ov.total_orders,
    cp.category AS top_category,
    -- Support tickets
    s.total_tickets
FROM crm_customer_raw_table c
LEFT JOIN eng_time e 
    ON e.customer_email = c.email
LEFT JOIN order_val ov 
    ON ov.customer_id = c.crm_customer_id
LEFT JOIN cat_pref cp 
    ON cp.customer_id = c.crm_customer_id
LEFT JOIN support s 
    ON s.customer_email = c.email;





select * from view_campaign_response;






DROP VIEW IF EXISTS vw_campaign_response; 
-- Replace table/column names as needed:
-- customers(customer_id, name, ...)
-- engagements(customer_id, event_type, engagement_date, campaign_id) -- event_type in ('open','click',...)
-- orders(customer_id, order_amount, order_date)
-- loyalty(customer_id, tier)
-- category_affinity(customer_id, category)

CREATE VIEW IF NOT EXISTS vw_campaign_response_features AS
SELECT
  c.customer_id,
  -- past opens & clicks (total counts)
  COALESCE(SUM(CASE WHEN e.event_type = 'open' THEN 1 ELSE 0 END), 0)  AS past_opens,
  COALESCE(SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END), 0) AS past_clicks,
  -- loyalty tier (text). change to numeric mapping if required.
  COALESCE(l.tier, 'unknown') AS loyalty_tier,
  -- simple category affinity: comma-separated top categories bought / interacted
  COALESCE(a.top_categories, 'none') AS category_affinity,
  -- past order value: total historic order amount
  COALESCE(o.total_order_value, 0.0) AS past_order_value,
  -- days since last engagement (NULL if never engaged)
  CASE
    WHEN MAX(e.engagement_date) IS NOT NULL
      THEN (julianday('now') - julianday(MAX(e.engagement_date)))
    ELSE NULL
  END AS days_since_last_engagement,
  -- label: 1 if customer has a click record (adjust logic to filter by campaign if needed)
  CASE WHEN SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS label
FROM customers c
LEFT JOIN engagements e
  ON e.customer_id = c.customer_id
  -- If you want label/features restricted to a particular campaign window, add:
  -- AND e.campaign_id = <YOUR_CAMPAIGN_ID>
LEFT JOIN (
  SELECT customer_id, SUM(order_amount) AS total_order_value
  FROM orders
  GROUP BY customer_id
) o ON o.customer_id = c.customer_id
LEFT JOIN loyalty l
  ON l.customer_id = c.customer_id
LEFT JOIN (
  -- build a compact affinity string (change to score if you prefer)
  SELECT customer_id, GROUP_CONCAT(category, ',') AS top_categories
  FROM category_affinity
  GROUP BY customer_id
) a ON a.customer_id = c.customer_id
GROUP BY c.customer_id;

select * from vw_campaign_response_features;








DROP VIEW IF EXISTS vw_campaign_response_features;
CREATE VIEW vw_campaign_response_features AS
WITH past_engagement AS (
    SELECT
        me.customer_email,
        COUNT(CASE WHEN LOWER(me.event_type) = 'open' THEN 1 END) AS past_opens,
        COUNT(CASE WHEN LOWER(me.event_type) = 'click' THEN 1 END) AS past_clicks,
        MAX(me.event_time) AS last_engagement_time
    FROM marketing_event_raw_table me
    GROUP BY me.customer_email
),
order_stats AS (
    SELECT
        o.customer_id,
        SUM(o.net_amount) AS total_order_value
    FROM raw_ecommerce_orders_raw_table o
    GROUP BY o.customer_id
),
ticket_stats AS (
    SELECT
        st.customer_email,
        COUNT(*) AS total_tickets
    FROM raw_support_tickets_raw_table st
    GROUP BY st.customer_email
)
SELECT
    c.crm_customer_id AS customer_id,
    -- Removed category_affinity
    c.email,
    COALESCE(pe.past_opens, 0) AS past_opens,
    COALESCE(pe.past_clicks, 0) AS past_clicks,
    COALESCE(os.total_order_value, 0) AS past_order_value,
    COALESCE(ts.total_tickets, 0) AS total_tickets,
    CASE
        WHEN me.event_type IS NOT NULL AND LOWER(me.event_type) = 'click'
        THEN 1 ELSE 0
    END AS clicked_flag
FROM crm_customer_raw_table c
LEFT JOIN past_engagement pe
       ON pe.customer_email = c.email
LEFT JOIN order_stats os
       ON os.customer_id = c.crm_customer_id
LEFT JOIN ticket_stats ts
       ON ts.customer_email = c.email
LEFT JOIN marketing_event_raw_table me
       ON me.customer_email = c.email;
select * from vw_campaign_response_features;

