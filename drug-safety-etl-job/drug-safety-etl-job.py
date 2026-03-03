"""
Drug Adverse Event Monitoring Pipeline
AWS Glue ETL Job — Bronze → Silver → Gold
Medallion Architecture: Raw JSON → Enriched Parquet → Analytics Parquet

This script:
1. Reads simulated CDC JSON files from S3 bronze/
2. Joins adverse_events with patients and drugs
3. Engineers features: severity scores, risk tiers, FDA reporting flags
4. Writes clean Parquet to silver/ and gold/
"""

import sys
import json
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, StringType

# ── Job parameters ────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET"])
SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"[INFO] Starting ETL job: {args['JOB_NAME']}")
print(f"[INFO] Source: s3://{SOURCE_BUCKET}/bronze/")
print(f"[INFO] Target: s3://{TARGET_BUCKET}/silver/ and gold/")

# ── Step 1: Read Bronze JSON Files ────────────────────────────────────────────
print("[INFO] Reading bronze JSON files...")

adverse_df = spark.read.json(f"s3://{SOURCE_BUCKET}/bronze/adverse_events/")
patients_df = spark.read.json(f"s3://{SOURCE_BUCKET}/bronze/patients/")
drugs_df = spark.read.json(f"s3://{SOURCE_BUCKET}/bronze/drugs/")

print(f"[INFO] Adverse events: {adverse_df.count()} rows")
print(f"[INFO] Patients: {patients_df.count()} rows")
print(f"[INFO] Drugs: {drugs_df.count()} rows")

# ── Step 2: Feature Engineering — Severity Score ──────────────────────────────
print("[INFO] Computing severity scores...")

# Symptom severity weights (clinical scoring)
SYMPTOM_SCORES = {
    "cardiac arrest": 10,
    "stroke": 10,
    "anaphylaxis": 9,
    "respiratory failure": 9,
    "liver failure": 8,
    "kidney failure": 8,
    "seizure": 7,
    "bleeding": 6,
    "paralysis": 6,
    "sepsis": 8,
    "death": 10,
    "hospitalized": 5,
    "chest pain": 5,
    "jaundice": 4,
    "edema": 4,
    "rash": 2,
    "nausea": 1,
    "vomiting": 1,
    "headache": 1,
    "fatigue": 1,
    "dizziness": 1,
    "insomnia": 1,
    "anxiety": 1,
    "itching": 1,
    "swelling": 2,
    "bruising": 3,
    "breathing difficulty": 7,
}

# Outcome severity weights
OUTCOME_SCORES = {
    "death": 20,
    "life-threatening": 15,
    "hospitalized": 8,
    "severe": 6,
    "moderate": 3,
    "mild": 1,
    "recovered": 0,
}

@F.udf(returnType=IntegerType())
def compute_symptom_score(symptoms_str):
    """Score symptoms based on clinical severity weights."""
    if not symptoms_str:
        return 0
    symptoms = [s.strip().lower() for s in symptoms_str.split(",")]
    return sum(SYMPTOM_SCORES.get(s, 0) for s in symptoms)

@F.udf(returnType=IntegerType())
def compute_outcome_score(outcome_str):
    """Score outcome based on clinical severity."""
    if not outcome_str:
        return 0
    return OUTCOME_SCORES.get(outcome_str.strip().lower(), 0)

adverse_df = adverse_df.withColumn(
    "symptom_score", compute_symptom_score(F.col("symptoms"))
).withColumn(
    "outcome_score", compute_outcome_score(F.col("outcome"))
).withColumn(
    "severity_score", F.col("symptom_score") + F.col("outcome_score")
)

# ── Step 3: Join with Patients ────────────────────────────────────────────────
print("[INFO] Joining with patient data...")

@F.udf(returnType=IntegerType())
def compute_patient_risk(age, has_diabetes, has_hypertension, has_kidney_disease):
    """Add risk points based on patient comorbidities."""
    score = 0
    if age and age >= 65:
        score += 3
    elif age and age >= 50:
        score += 1
    if has_diabetes:
        score += 2
    if has_hypertension:
        score += 1
    if has_kidney_disease:
        score += 3
    return score

patients_df = patients_df.withColumn(
    "patient_risk_score",
    compute_patient_risk(
        F.col("age"),
        F.col("has_diabetes"),
        F.col("has_hypertension"),
        F.col("has_kidney_disease")
    )
)

enriched_df = adverse_df.join(
    patients_df.select("patient_id", "age", "gender", "has_diabetes",
                        "has_hypertension", "has_kidney_disease", "patient_risk_score"),
    on="patient_id",
    how="left"
)

# ── Step 4: Join with Drugs ───────────────────────────────────────────────────
print("[INFO] Joining with drug data...")

DRUG_CLASS_RISK = {
    "anticoagulant": 4,
    "immunosuppressant": 4,
    "chemotherapy": 5,
    "antiplatelet": 3,
    "ace_inhibitor": 2,
    "corticosteroid": 2,
    "ssri": 2,
    "antibiotic": 1,
    "antidiabetic": 1,
}

@F.udf(returnType=IntegerType())
def compute_drug_risk(drug_class, black_box_warning):
    """Add risk points based on drug class and black box warning."""
    score = DRUG_CLASS_RISK.get(drug_class.lower() if drug_class else "", 0)
    if black_box_warning:
        score += 3
    return score

drugs_df = drugs_df.withColumn(
    "drug_risk_score",
    compute_drug_risk(F.col("drug_class"), F.col("black_box_warning"))
)

enriched_df = enriched_df.join(
    drugs_df.select("drug_id", "drug_name", "drug_class",
                     "manufacturer", "black_box_warning", "drug_risk_score"),
    on="drug_id",
    how="left"
)

# ── Step 5: Final Severity Score and Risk Tier ────────────────────────────────
print("[INFO] Computing final risk tiers...")

enriched_df = enriched_df.withColumn(
    "total_score",
    F.col("severity_score") +
    F.coalesce(F.col("patient_risk_score"), F.lit(0)) +
    F.coalesce(F.col("drug_risk_score"), F.lit(0))
).withColumn(
    "risk_tier",
    F.when(F.col("total_score") >= 30, "CRITICAL")
     .when(F.col("total_score") >= 20, "HIGH")
     .when(F.col("total_score") >= 10, "MEDIUM")
     .otherwise("LOW")
)

# ── Step 6: FDA Reporting Flags ───────────────────────────────────────────────
print("[INFO] Computing FDA reporting deadlines...")

# FDA requires reporting within 15 days for serious adverse events
@F.udf(returnType=BooleanType())
def is_fda_reportable(severity, outcome, total_score):
    """Flag events that must be reported to FDA within 15 days."""
    serious_severities = {"life-threatening", "severe"}
    serious_outcomes = {"death", "hospitalized"}
    if severity and severity.lower() in serious_severities:
        return True
    if outcome and outcome.lower() in serious_outcomes:
        return True
    if total_score and total_score >= 20:
        return True
    return False

@F.udf(returnType=StringType())
def compute_reporting_deadline(event_date_str, fda_reportable):
    """Compute 15-day FDA reporting deadline."""
    if not fda_reportable or not event_date_str:
        return None
    try:
        event_date = datetime.strptime(event_date_str[:10], "%Y-%m-%d")
        deadline = event_date + timedelta(days=15)
        return deadline.strftime("%Y-%m-%d")
    except Exception:
        return None

enriched_df = enriched_df.withColumn(
    "fda_reportable",
    is_fda_reportable(F.col("severity"), F.col("outcome"), F.col("total_score"))
).withColumn(
    "reporting_deadline",
    compute_reporting_deadline(F.col("event_date"), F.col("fda_reportable"))
)

# Clean up intermediate columns
silver_df = enriched_df.drop("symptom_score", "outcome_score")

print(f"[INFO] Silver dataset: {silver_df.count()} enriched records")

# ── Step 7: Write Silver Layer ────────────────────────────────────────────────
print("[INFO] Writing silver layer (Parquet)...")

silver_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"s3://{TARGET_BUCKET}/silver/adverse_events_enriched/")

print("[INFO] Silver layer written successfully.")

# ── Step 8: Build Gold Layer — Safety Signals ─────────────────────────────────
print("[INFO] Building gold layer — safety signals...")

safety_signals_df = silver_df.groupBy("drug_id", "drug_name", "drug_class") \
    .agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("outcome") == "death", 1).otherwise(0)).alias("deaths"),
        F.sum(F.when(F.col("risk_tier").isin("HIGH", "CRITICAL"), 1).otherwise(0)).alias("high_risk_events"),
        F.sum(F.when(F.col("fda_reportable") == True, 1).otherwise(0)).alias("fda_reportable_count"),
        F.avg("total_score").alias("avg_severity_score"),
        F.max("total_score").alias("max_severity_score"),
        F.countDistinct("patient_id").alias("unique_patients_affected")
    ) \
    .withColumn(
        "signal_strength",
        F.when(F.col("deaths") > 0, "STRONG")
         .when(F.col("high_risk_events") >= 2, "MODERATE")
         .when(F.col("total_events") >= 2, "WEAK")
         .otherwise("MINIMAL")
    ) \
    .withColumn(
        "requires_investigation",
        (F.col("deaths") > 0) | (F.col("high_risk_events") >= 2)
    )

safety_signals_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"s3://{TARGET_BUCKET}/gold/safety_signals/")

print("[INFO] Safety signals written.")

# ── Step 9: Build Gold Layer — FDA Reportable Events ─────────────────────────
print("[INFO] Building gold layer — FDA reportable events...")

fda_df = silver_df.filter(F.col("fda_reportable") == True) \
    .select(
        "patient_id", "drug_id", "drug_name", "event_date",
        "symptoms", "severity", "outcome", "total_score",
        "risk_tier", "reporting_deadline", "age", "gender",
        "black_box_warning", "reported_by"
    ) \
    .withColumn(
        "days_until_deadline",
        F.datediff(
            F.to_date(F.col("reporting_deadline"), "yyyy-MM-dd"),
            F.current_date()
        )
    ) \
    .orderBy("reporting_deadline")

fda_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"s3://{TARGET_BUCKET}/gold/fda_reportable_events/")

print(f"[INFO] FDA reportable events: {fda_df.count()} events written.")

# ── Step 10: Build Gold Layer — Monthly Trends ────────────────────────────────
print("[INFO] Building gold layer — monthly trends...")

trends_df = silver_df \
    .withColumn("year_month", F.date_format(F.to_date("event_date"), "yyyy-MM")) \
    .groupBy("year_month") \
    .agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("outcome") == "death", 1).otherwise(0)).alias("deaths"),
        F.sum(F.when(F.col("risk_tier").isin("HIGH", "CRITICAL"), 1).otherwise(0)).alias("high_risk_events"),
        F.avg("total_score").alias("avg_severity"),
        F.sum(F.when(F.col("fda_reportable") == True, 1).otherwise(0)).alias("fda_reportable_events")
    ) \
    .orderBy("year_month")

trends_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"s3://{TARGET_BUCKET}/gold/monthly_trends/")

print("[INFO] Monthly trends written.")

# ── Done ──────────────────────────────────────────────────────────────────────
print("[INFO] ✅ ETL job completed successfully!")
print(f"[INFO]   Silver: s3://{TARGET_BUCKET}/silver/adverse_events_enriched/")
print(f"[INFO]   Gold:   s3://{TARGET_BUCKET}/gold/safety_signals/")
print(f"[INFO]   Gold:   s3://{TARGET_BUCKET}/gold/fda_reportable_events/")
print(f"[INFO]   Gold:   s3://{TARGET_BUCKET}/gold/monthly_trends/")

job.commit()
