import pandas as pd
from datetime import datetime
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

AS_AT_DATE = pd.to_datetime("2025-07-07")


def load_data():
    try:
        invoices = pd.read_csv("data/invoices.csv", parse_dates=["invoice_date"])
        credit_notes = pd.read_csv("data/credit_notes.csv", parse_dates=["credit_note_date"])
        payments = pd.read_csv("data/payments.csv", parse_dates=["payment_date"])
        return invoices, credit_notes, payments
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def process_documents(documents, doc_type, payments):
    # Rename date column
    date_col = "invoice_date" if doc_type == "invoice" else "credit_note_date"
    documents = documents.rename(columns={date_col: "document_date", "id": "document_id"})
    documents["document_type"] = doc_type

    # Merge payments
    relevant_payments = payments[payments["document_type"] == doc_type]
    payment_sums = relevant_payments.groupby("document_id")["amount_paid"].sum().reset_index()
    payment_sums.columns = ["document_id", "amount_paid"]

    merged = pd.merge(documents, payment_sums, on="document_id", how="left").fillna(0)
    merged["outstanding"] = merged["total_amount"] - merged["amount_paid"]

    # Filter by balance > 0
    merged = merged[merged["outstanding"] > 0].copy()
    return merged


def assign_bucket(days_diff):
    if 0 <= days_diff <= 30:
        return "day_30"
    elif 31 <= days_diff <= 60:
        return "day_60"
    elif 61 <= days_diff <= 90:
        return "day_90"
    elif 91 <= days_diff <= 120:
        return "day_120"
    elif 121 <= days_diff <= 150:
        return "day_150"
    elif 151 <= days_diff <= 180:
        return "day_180"
    else:
        return "day_180_and_above"


def build_ageing_fact():
    invoices, credit_notes, payments = load_data()

    inv_df = process_documents(invoices, "invoice", payments)
    cr_df = process_documents(credit_notes, "credit_note", payments)

    combined = pd.concat([inv_df, cr_df], ignore_index=True)
    combined["as_at_date"] = AS_AT_DATE
    combined["days_diff"] = (AS_AT_DATE - combined["document_date"]).dt.days
    combined["bucket"] = combined["days_diff"].apply(assign_bucket)

    # Prepare final schema with only one ageing bucket populated
    bucket_cols = ["day_30", "day_60", "day_90", "day_120", "day_150", "day_180", "day_180_and_above"]
    for col in bucket_cols:
        combined[col] = combined.apply(lambda row: row["outstanding"] if row["bucket"] == col else 0.0, axis=1)

    final_cols = [
        "centre_id", "class_id", "document_id", "document_date", "student_id",
        *bucket_cols, "document_type", "as_at_date"
    ]
    final_df = combined[final_cols]
    return final_df


if __name__ == "__main__":
    logging.info("Starting ageing fact table pipeline...")
    result_df = build_ageing_fact()

    os.makedirs("output", exist_ok=True)
    result_df.to_csv("output/ageing_fact_table.csv", index=False)
    logging.info("Ageing fact table written to output/ageing_fact_table.csv")
