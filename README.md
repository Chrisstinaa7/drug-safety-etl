# 💊 Drug Adverse Event Monitoring Pipeline

> A production-grade pharmacovigilance data pipeline built on AWS — automating adverse drug event ingestion, composite risk scoring, FDA compliance flagging, and safety signal detection across a Bronze → Silver → Gold medallion architecture.

---

## 📐 Architecture

```
You (upload JSON)
       │
       ▼
┌─────────────────┐
│   S3 Bronze     │  Raw CDC-format JSON  (adverse_events / patients / drugs)
└────────┬────────┘
         │
         ▼  AWS Glue ETL (PySpark)
┌─────────────────┐
│   S3 Silver     │  Enriched Parquet — composite severity scores, risk tiers,
│                 │  patient comorbidity context, FDA deadlines
└────────┬────────┘
         │
         ▼  AWS Glue ETL (PySpark)
┌─────────────────┐
│    S3 Gold      │  Aggregated Parquet — safety signals, FDA reportable events,
│                 │  monthly trends
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Amazon Athena  │  10 analytics SQL queries — risk dashboards, FDA deadlines,
│                 │  investigation lists, drug class risk
└─────────────────┘
         ▲
         │  Orchestrated by
┌─────────────────┐
│ Step Functions  │  Glue → MSCK REPAIR ×3 → Validation → FDA Check
│  + EventBridge  │  Scheduled, retried, fully auditable
└─────────────────┘
```

**AWS Services:** S3 · Glue · Athena · Step Functions · EventBridge · IAM · CloudWatch

> **Note on DMS:** AWS DMS was intentionally removed from this pipeline. DMS costs ~$0.43/day when idle and requires RDS logical replication setup. Instead, DMS-format JSON files are uploaded directly to S3 bronze/ — Glue processes them identically. This is a deliberate cost-optimization pattern; the architecture fully supports DMS as a drop-in replacement for the upload step.

---

## 🔬 What the Pipeline Does

### Feature Engineering (Silver Layer)
Every adverse event is scored across four independent risk dimensions:

| Dimension | Logic | Max Points |
|---|---|---|
| **Symptom severity** | Clinical weights per symptom (cardiac arrest=10, stroke=10, kidney failure=8…) | ~30 |
| **Outcome severity** | death=20, life-threatening=15, hospitalized=8, severe=6… | 20 |
| **Patient risk** | Age 65+=3, kidney disease=3, diabetes=2, hypertension=1 | 9 |
| **Drug class risk** | Anticoagulant=4, immunosuppressant=4, black box warning=+3 | 7 |

```
total_score = symptom_score + outcome_score + patient_risk_score + drug_risk_score
```

**Risk Tiers:**

| Score | Tier | Action |
|---|---|---|
| ≥ 30 | `CRITICAL` | Immediate review |
| 20–29 | `HIGH` | Priority investigation |
| 10–19 | `MEDIUM` | Standard monitoring |
| < 10 | `LOW` | Routine logging |

### FDA Compliance Automation
Events are automatically flagged `fda_reportable = true` if:
- `severity` is `life-threatening` or `severe`
- `outcome` is `death` or `hospitalized`  
- `total_score >= 20`

A 15-day reporting deadline is computed and stored in the gold layer. The Step Functions pipeline runs an FDA check query on every execution.

### Safety Signal Detection (Gold Layer)
Drug-level aggregation from the enriched silver layer:

| Signal Strength | Condition |
|---|---|
| `STRONG` | Any death associated with the drug |
| `MODERATE` | 2+ high-risk events |
| `WEAK` | 2+ total events |
| `MINIMAL` | Everything else |

`requires_investigation` flag set for `STRONG` and `MODERATE` signals.

---

## 📁 Repository Structure

```
drug-adverse-event-pipeline/
│
├── README.md
│
├── data/
│   └── bronze/
│       ├── adverse_events.json     # 10 sample adverse event records (CDC format)
│       ├── patients.json           # 8 patient records with comorbidity data
│       └── drugs.json              # 8 drug records with class and warning data
│
├── glue/
│   └── etl_job.py                  # PySpark ETL — Bronze → Silver → Gold
│
├── athena/
│   └── queries.sql                 # CREATE TABLE statements + 10 analytics queries
│
├── stepfunctions/
│   └── pipeline_definition.json   # ASL state machine definition
│
└── docs/
    └── setup-guide.docx            # Complete setup guide (No DMS version)
```

---

## 🚀 Quick Start

### Prerequisites
- AWS account with IAM user (AdministratorAccess)
- AWS CLI configured (`aws configure`)
- Same AWS region for all services (guide uses `ap-south-1`)

### Step 1 — Create S3 Bucket

```bash
aws s3 mb s3://drug-safety-pipeline-YOUR-NAME --region ap-south-1

# Create folder structure
aws s3api put-object --bucket drug-safety-pipeline-YOUR-NAME --key bronze/
aws s3api put-object --bucket drug-safety-pipeline-YOUR-NAME --key silver/
aws s3api put-object --bucket drug-safety-pipeline-YOUR-NAME --key gold/
aws s3api put-object --bucket drug-safety-pipeline-YOUR-NAME --key athena-results/
aws s3api put-object --bucket drug-safety-pipeline-YOUR-NAME --key glue-scripts/
```

### Step 2 — Upload Bronze Data and Glue Script

```bash
# Upload simulated CDC data
aws s3 cp data/bronze/adverse_events.json s3://drug-safety-pipeline-YOUR-NAME/bronze/adverse_events/
aws s3 cp data/bronze/patients.json       s3://drug-safety-pipeline-YOUR-NAME/bronze/patients/
aws s3 cp data/bronze/drugs.json          s3://drug-safety-pipeline-YOUR-NAME/bronze/drugs/

# Upload ETL script
aws s3 cp glue/etl_job.py s3://drug-safety-pipeline-YOUR-NAME/glue-scripts/
```

### Step 3 — Create IAM Roles

**Glue role:**
```bash
aws iam create-role --role-name glue-etl-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam attach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

**Step Functions role:**
```bash
aws iam create-role --role-name stepfunctions-pipeline-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
```

### Step 4 — Create the Glue ETL Job

In the AWS Console:
1. **Glue → ETL Jobs → Script editor**
2. Upload `glue/etl_job.py`
3. Set job name: `drug-safety-etl-job`
4. IAM Role: `glue-etl-role`
5. Glue version: `4.0` · Worker type: `G.1X` · Workers: `2` · Timeout: `30 min`
6. Add job parameters:

| Key | Value |
|---|---|
| `--SOURCE_BUCKET` | `drug-safety-pipeline-YOUR-NAME` |
| `--TARGET_BUCKET` | `drug-safety-pipeline-YOUR-NAME` |

7. **Save → Run**

### Step 5 — Set Up Athena

1. **Athena → Settings → Manage**
2. Query result location: `s3://drug-safety-pipeline-YOUR-NAME/athena-results/`
3. Run setup queries from `athena/queries.sql` (replace `YOUR-BUCKET-NAME`)
4. Run `MSCK REPAIR TABLE` for each table after first Glue run

### Step 6 — Deploy Step Functions

```bash
# Replace YOUR-BUCKET-NAME and YOUR-ACCOUNT-ID in pipeline_definition.json first
aws stepfunctions create-state-machine \
  --name drug-safety-pipeline \
  --definition file://stepfunctions/pipeline_definition.json \
  --role-arn arn:aws:iam::YOUR-ACCOUNT-ID:role/stepfunctions-pipeline-role \
  --region ap-south-1
```

---

## 📊 Athena Analytics Queries

| Query | Description |
|---|---|
| `Q1` | Dashboard summary — event counts by risk tier |
| `Q2` | Most dangerous drugs — ordered by deaths and signal strength |
| `Q3` | FDA deadlines — events with urgency countdown |
| `Q4` | Risk by drug class — which categories drive the most severity |
| `Q5` | Elderly high-risk events — age 65+ with comorbidities |
| `Q6` | Monthly trends — events over time with MoM % change |
| `Q7` | Most common symptoms in HIGH/CRITICAL events |
| `Q8` | **Investigation list** — drugs requiring immediate safety review |
| `Q9` | Comorbidity risk profile — how conditions compound severity |
| `Q10` | Black box warning analysis — warnings vs outcomes |

---

## 🔄 Simulating a New Adverse Event

To test the end-to-end pipeline without DMS:

```bash
# 1. Create a new event file
cat > new_event.json << 'EOF'
{"patient_id": 7, "drug_id": 4, "event_date": "2024-03-01", "symptoms": "cardiac arrest,bleeding", "severity": "life-threatening", "outcome": "death", "reported_by": "physician"}
EOF

# 2. Upload to bronze
aws s3 cp new_event.json s3://drug-safety-pipeline-YOUR-NAME/bronze/adverse_events/

# 3. Trigger the pipeline
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:ap-south-1:YOUR-ACCOUNT-ID:stateMachine:drug-safety-pipeline \
  --input '{}'

# 4. Query Athena after pipeline completes
# Run Q8 — the new event should appear in the investigation list
```

---

## 🗂️ Data Schema

### Bronze — `adverse_events.json`
```json
{
  "patient_id": 2,
  "drug_id": 1,
  "event_date": "2024-01-18",
  "symptoms": "cardiac arrest,chest pain",
  "severity": "life-threatening",
  "outcome": "death",
  "reported_by": "physician"
}
```

### Bronze — `patients.json`
```json
{
  "patient_id": 7,
  "age": 81,
  "gender": "F",
  "weight_kg": 55,
  "has_diabetes": true,
  "has_hypertension": true,
  "has_kidney_disease": true
}
```

### Bronze — `drugs.json`
```json
{
  "drug_id": 1,
  "drug_name": "Warfarin",
  "drug_class": "anticoagulant",
  "manufacturer": "PharmaCo",
  "approval_year": 1954,
  "black_box_warning": true
}
```

### Silver — `adverse_events_enriched` (Parquet)
Original bronze fields + `age`, `gender`, `has_diabetes`, `has_hypertension`, `has_kidney_disease`, `patient_risk_score`, `drug_name`, `drug_class`, `black_box_warning`, `drug_risk_score`, `severity_score`, `total_score`, `risk_tier`, `fda_reportable`, `reporting_deadline`

### Gold — `safety_signals` (Parquet)
`drug_id`, `drug_name`, `drug_class`, `total_events`, `deaths`, `high_risk_events`, `fda_reportable_count`, `avg_severity_score`, `max_severity_score`, `unique_patients_affected`, `signal_strength`, `requires_investigation`

---

## 💰 Cost Breakdown

| Resource | Cost |
|---|---|
| S3 storage | ~$0.02/month |
| Glue ETL | ~$0.10 per job run |
| Athena queries | Fractions of a cent (small dataset) |
| Step Functions | Free (< 4,000 state transitions/month) |
| EventBridge | Free |
| **Total idle cost** | **$0.00/day** |

> No DMS or RDS = no idle charges. You only spend money when you actively run the pipeline.

---

## 🧹 Cleanup

```bash
# Empty and delete S3 bucket
aws s3 rm s3://drug-safety-pipeline-YOUR-NAME --recursive
aws s3 rb s3://drug-safety-pipeline-YOUR-NAME

# Delete Glue job
aws glue delete-job --job-name drug-safety-etl-job

# Delete Step Functions state machine
aws stepfunctions delete-state-machine \
  --state-machine-arn arn:aws:states:ap-south-1:YOUR-ACCOUNT-ID:stateMachine:drug-safety-pipeline

# Delete IAM roles
aws iam detach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam detach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam detach-role-policy --role-name glue-etl-role --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
aws iam delete-role --role-name glue-etl-role

aws iam detach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam detach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam detach-role-policy --role-name stepfunctions-pipeline-role --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
aws iam delete-role --role-name stepfunctions-pipeline-role
```

---

## 🛠️ Troubleshooting

| Problem | Solution |
|---|---|
| Glue job fails: `NoSuchKey` | Verify `bronze/adverse_events/`, `bronze/patients/`, `bronze/drugs/` folders each contain a `.json` file |
| Glue job fails: `Access Denied` | Check `glue-etl-role` has `AmazonS3FullAccess` attached |
| Athena returns 0 rows | Run `MSCK REPAIR TABLE drug_safety.adverse_events_enriched` — new Parquet partitions not registered |
| Step Functions fails at Glue step | Confirm Glue job name in `pipeline_definition.json` exactly matches `drug-safety-etl-job` |
| Step Functions fails at Athena step | Confirm Athena tables were created first via `queries.sql` setup section |
| `silver/` empty after Glue run | Check CloudWatch → `/aws-glue/jobs/output` for `[INFO]` row count lines |

---

## 🧠 Key Concepts

**Medallion Architecture** — Bronze (raw) → Silver (enriched) → Gold (aggregated). Each layer adds value; reprocess downstream layers without re-ingesting source data.

**Composite Risk Scoring** — Events scored across four independent dimensions (symptom, outcome, patient, drug). No single dimension needs to be extreme — the combination determines the tier.

**CDC Simulation** — DMS-format JSON (one record per line) uploaded to S3 bronze/ is functionally identical to live DMS output. Glue processes both the same way.

**MSCK REPAIR TABLE** — Required after every Glue run to register new Parquet partitions in the Athena/Glue Data Catalog. Without it, Athena returns zero rows even though the data exists in S3.

**Idempotency** — Glue writes with `.mode("overwrite")`. Re-running the pipeline produces identical output; no duplicate records accumulate.

---

## 📄 License

MIT — free to use, modify, and distribute.

---

*Built as a portfolio project demonstrating production-grade AWS data engineering patterns. All data is synthetically generated.*
