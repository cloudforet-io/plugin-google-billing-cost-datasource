# Google Cloud Billing Cost DataSource Plugin

A plugin that collects Google Cloud Platform billing data for the SpaceONE platform.

## Overview

This plugin can collect GCP billing data from the following sources:
- **BigQuery**: Direct queries from GCP Billing Export tables
- **Google Cloud Storage**: Billing Export files (CSV, Parquet, Avro formats)

## Configuration Guide

### Step 1: Plugin Registration

First, you need to register the plugin in SpaceONE.

**register_plugin.yaml example:**
```yaml
capability: {}
image: <docker_image_name>
labels:
  - Cost
  - GCP
  - BigQuery
  - GCS Billing File
  - Google Cost DataSource
name: <plugin_name>
plugin_id: <unique_plugin_id>
provider: google_cloud
registry_config:
  image_pull_secret: <image_pull_secret>  # Docker image authentication secret
  url: <docker_registry_url>              # Docker registry URL
registry_type: DOCKER_HUB                # DOCKER_HUB, GCP_PRIVATE_GCR, AWS_PRIVATE_ECR
resource_type: cost_analysis.DataSource
tags: {}
```

**Registration command:**
```bash
spacectl exec register repository.Plugin -f register_plugin.yaml
```

### Step 2: DataSource Registration

Register the plugin as a data source to enable actual billing data collection.

**register_datasource.yaml example:**
```yaml
name: <datasource_name>
data_source_type: EXTERNAL
provider: google_cloud
secret_type: MANUAL
plugin_info:
  plugin_id: <plugin_id>
  version: <version>
  secret_data:  # Google Cloud Service Account key information
    auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
    auth_uri: https://accounts.google.com/o/oauth2/auth
    client_email: <service_account_email>
    client_id: <client_id>
    client_x509_cert_url: <certificate_url>
    private_key: <private_key>
    private_key_id: <key_id>
    project_id: <gcp_project_id>
    token_uri: https://oauth2.googleapis.com/token
    type: service_account
    universe_domain: googleapis.com
metadata:
  currency: USD  # Currency unit (USD, KRW, etc.)
  data_source_rules:
    - name: match_workspace
      actions:
        match_workspace:
          source: additional_info.Project ID
          target: data.project_id
      conditions_policy: ALWAYS
      options:
        stop_processing: true
  resource_group: DOMAIN
options:  # Plugin execution options
  source: bigquery                          # Data source: bigquery or gcs
  billing_export_project_id: <project_id>   # Project ID where billing export is stored
  billing_dataset_id: <dataset_id>          # BigQuery dataset ID
  billing_account_id: <billing_account_id>  # GCP billing account ID
  select_cost: list_price                   # Cost selection: list_price or actual_cost
  currency: USD                             # Currency unit
upgrade_mode: AUTO                          # Auto-update enabled
schedule:
  state: ENABLED                            # Schedule enabled
  hours: 16                                 # Execution time (0-23)
  resource_group: DOMAIN
tags: {}
```

**Registration command:**
```bash
spacectl exec register cost_analysis.DataSource -f register_datasource.yaml
```

## Detailed Configuration Options

### Data Source-Specific Settings

#### When using BigQuery source
```yaml
options:
  source: bigquery
  billing_export_project_id: <billing_export_project_id>
  billing_dataset_id: <bigquery_dataset_id>
  billing_account_id: <billing_account_id>
  selected_cost: list_price  # or actual_cost
  currency: USD
```

#### When using Google Cloud Storage source  
```yaml
options:
  source: gcs
  bucket_name: <gcs_bucket_name>
  project_id: <project_id>
  selected_cost: list_price  # or actual_cost
  currency: USD
```

### Key Configuration Items

| Option | Description | Required | Example |
|--------|-------------|----------|---------|
| `source` | Data source selection | Required | `bigquery`, `gcs` |
| `billing_export_project_id` | GCP project ID where billing export data is stored | Required | `my-billing-project` |
| `billing_dataset_id` | BigQuery dataset ID (for BigQuery source) | Required for BigQuery | `billing_data` |
| `billing_account_id` | GCP billing account ID | Required | `01AB23-CD45EF-GH69IJ` |
| `bucket_name` | GCS bucket name (for GCS source) | Required for GCS | `my-billing-bucket` |
| `project_id` | GCS project ID  (for GCS source) | Required for GCS | `project_id` |
| `select_cost` | Cost data selection criteria | Required | `list_price` |
| `currency` | Currency unit | Required | `USD`, `KRW` |

## Prerequisites

### GCP Configuration
1. **Enable Billing Export**: Set up Cloud Billing Export to BigQuery or GCS in the GCP console
2. **Create Service Account**: Create a service account with the following permissions:
   - For BigQuery: `BigQuery Data Viewer`, `BigQuery Job User`
   - For GCS: `Storage Object Viewer`
3. **Generate Service Account Key**: Create a JSON format key file

### Billing Export Table Format
BigQuery table names must follow this pattern:
```
{billing_export_project_id}.{billing_dataset_id}.gcp_billing_export_v1_{billing_account_id}
```

## Troubleshooting

### Common Errors
- **Authentication Error**: Check your service account key and permissions
- **Table Not Found**: Verify your billing account ID and export settings
- **No Data Collected**: Ensure billing export is enabled and data exists