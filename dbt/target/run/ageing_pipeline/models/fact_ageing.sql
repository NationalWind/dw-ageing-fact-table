
  
    

  create  table "ageing"."public"."fact_ageing__dbt_tmp"
  
  
    as
  
  (
    

WITH union_docs AS (
    -- Combine invoices
    SELECT
        i.id AS document_id,
        i.centre_id,
        i.class_id,
        i.student_id,
        i.invoice_date AS document_date,
        'invoice' AS document_type,
        i.total_amount
    FROM accounting.invoices i

    UNION ALL

    -- Combine credit_notes
    SELECT
        c.id,
        c.centre_id,
        c.class_id,
        c.student_id,
        c.credit_note_date AS document_date,
        'credit_note',
        c.total_amount
    FROM accounting.credit_notes c
),

payments_agg AS (
    SELECT
        document_id,
        document_type,
        SUM(amount_paid) AS amount_paid
    FROM accounting.payments
    GROUP BY document_id, document_type
),

docs_with_balance AS (
    SELECT
        d.*,
        COALESCE(p.amount_paid, 0) AS amount_paid,
        d.total_amount - COALESCE(p.amount_paid, 0) AS outstanding_amount
    FROM union_docs d
    LEFT JOIN payments_agg p
        ON d.document_id = p.document_id AND d.document_type = p.document_type
    WHERE (d.total_amount - COALESCE(p.amount_paid, 0)) > 0
),

with_days_bucket AS (
    SELECT
        *,
        DATE '2025-07-07' AS as_at_date,
        DATE '2025-07-07' - document_date AS days_outstanding
    FROM docs_with_balance
),

bucketed AS (
    SELECT
        *,
        CASE WHEN days_outstanding BETWEEN 0 AND 30 THEN outstanding_amount ELSE 0.00 END AS day_30,
        CASE WHEN days_outstanding BETWEEN 31 AND 60 THEN outstanding_amount ELSE 0.00 END AS day_60,
        CASE WHEN days_outstanding BETWEEN 61 AND 90 THEN outstanding_amount ELSE 0.00 END AS day_90,
        CASE WHEN days_outstanding BETWEEN 91 AND 120 THEN outstanding_amount ELSE 0.00 END AS day_120,
        CASE WHEN days_outstanding BETWEEN 121 AND 150 THEN outstanding_amount ELSE 0.00 END AS day_150,
        CASE WHEN days_outstanding BETWEEN 151 AND 180 THEN outstanding_amount ELSE 0.00 END AS day_180,
        CASE WHEN days_outstanding > 180 THEN outstanding_amount ELSE 0.00 END AS day_180_and_above
    FROM with_days_bucket
)

SELECT
    centre_id,
    class_id,
    document_id,
    document_date,
    student_id,
    day_30,
    day_60,
    day_90,
    day_120,
    day_150,
    day_180,
    day_180_and_above,
    document_type,
    as_at_date
FROM bucketed
  );
  