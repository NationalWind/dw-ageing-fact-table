CREATE SCHEMA IF NOT EXISTS accounting;

CREATE TABLE accounting.invoices (
    id TEXT PRIMARY KEY,
    centre_id TEXT,
    class_id TEXT,
    student_id TEXT,
    invoice_date DATE,
    total_amount NUMERIC(10,2)
);

CREATE TABLE accounting.credit_notes (
    id TEXT PRIMARY KEY,
    centre_id TEXT,
    class_id TEXT,
    student_id TEXT,
    credit_note_date DATE,
    total_amount NUMERIC(10,2)
);

CREATE TABLE accounting.payments (
    id TEXT PRIMARY KEY,
    document_id TEXT,
    document_type TEXT,
    amount_paid NUMERIC(10,2),
    payment_date DATE
);

INSERT INTO accounting.invoices
SELECT
    'inv_' || LPAD(i::TEXT, 4, '0'),
    'c_' || (i % 5 + 1),
    'cls_' || (i % 10 + 1),
    'stu_' || LPAD((i % 300 + 1)::TEXT, 4, '0'),
    CURRENT_DATE - (i % 250),
    ROUND((random() * 400 + 100)::numeric, 2)
FROM generate_series(1, 1000) AS s(i);

INSERT INTO accounting.credit_notes
SELECT
    'cr_' || LPAD(i::TEXT, 4, '0'),
    'c_' || (i % 5 + 1),
    'cls_' || (i % 10 + 1),
    'stu_' || LPAD((i % 300 + 1)::TEXT, 4, '0'),
    CURRENT_DATE - (i % 250),
    ROUND((random() * 300 + 50)::numeric, 2)
FROM generate_series(1, 1000) AS s(i);

INSERT INTO accounting.payments
SELECT
    'pay_' || LPAD(i::TEXT, 4, '0'),
    CASE WHEN i % 2 = 0 THEN 'inv_' || LPAD((i % 1000 + 1)::TEXT, 4, '0')
         ELSE 'cr_' || LPAD((i % 1000 + 1)::TEXT, 4, '0') END,
    CASE WHEN i % 2 = 0 THEN 'invoice' ELSE 'credit_note' END,
    ROUND((random() * 400 + 100)::numeric, 2),
    CURRENT_DATE - (i % 200)
FROM generate_series(1, 2000) AS s(i);
