# plugin-google-billing-cost-datasource

Plugin for collecting GCP Billing data from BigQuery service

---

## Secret Data

*Schema*

* project_id (str): project_id is a unique identifier for a project and is used only within the GCP console.
* private_key (str): When you add a GCP cloud account, you use a private key for a GCP service account
* token_uri (str): The OAuth 2.0 authorization server’s token endpoint URI.
* client_email (str): A service account's credentials include a generated email address
* billing_dataset (str)(**optional**): specifies the name of the dataset to collect billing data from. If not present, `spaceone_billing_data` is used.
* target_billing_account_id(str)(**optional**): specifies the billing_account_id to be collected. If not present, use billing_account_id linked to project_id in secret_data.
* target_project_id (list): specify the project_id to be collected by filtering the Cloud Billing data loaded into BigQuery by project_id. If `*` is specified, it is collected regardless of project_id. 

```
project_id, private_key, token_uri and client_email can be obtained from api_key issued when creating service_account.  

```

<br>

*Example*

```python
{
    "project_id": "<project_id>",
    "private_key": "*****",
    "client_email": "<service_account_name>@<project_id>.iam.gserviceaccount.com",
    "token_uri": "https://oauth2.googleapis.com/token",
    "billing_dataset": "spaceone_billing_data", #optional
    "target_billing_account_id": "{BILLING_ACCOUNT_ID}", #optional
    "target_project_id": ["*"]
}
```



<br>

## Options

Currently, not required.