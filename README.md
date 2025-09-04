# Google Cloud Billing Cost DataSource Plugin

Google Cloud Platform의 빌링 데이터를 SpaceONE 플랫폼으로 수집하는 플러그인입니다.

## 개요

이 플러그인은 다음 소스에서 GCP 빌링 데이터를 수집할 수 있습니다:
- **BigQuery**: GCP Billing Export 테이블에서 직접 조회
- **Google Cloud Storage**: Billing Export 파일 (CSV, Parquet, Avro 형식)

## 설정 가이드

### 1단계: 플러그인 등록

먼저 SpaceONE에 플러그인을 등록해야 합니다.

**register_plugin.yaml 예시:**
```yaml
capability: {}
image: <docker_image_name>
labels:
  - Cost
  - GCP
  - BigQuery
  - GCS Billing File
  - Google Cost DataSource
name: <플러그인_이름>
plugin_id: <플러그인_고유_ID>
provider: google_cloud
registry_config:
  image_pull_secret: <image_pull_secret>  # Docker 이미지 인증용 보안 키
  url: <docker_registry_url>              # Docker 레지스트리 URL
registry_type: DOCKER_HUB                # DOCKER_HUB, GCP_PRIVATE_GCR, AWS_PRIVATE_ECR
resource_type: cost_analysis.DataSource
tags: {}
```

**등록 명령어:**
```bash
spacectl exec register repository.Plugin -f register_plugin.yaml
```

### 2단계: 데이터소스 등록

플러그인을 데이터소스로 등록하여 실제 빌링 데이터를 수집할 수 있도록 설정합니다.

**register_datasource.yaml 예시:**
```yaml
name: <데이터소스_이름>
data_source_type: EXTERNAL
provider: google_cloud
secret_type: MANUAL
plugin_info:
  plugin_id: <플러그인_ID>
  version: <버전>
  secret_data:  # Google Cloud Service Account 키 정보
    auth_provider_x509_cert_url: https://www.googleapis.com/oauth2/v1/certs
    auth_uri: https://accounts.google.com/o/oauth2/auth
    client_email: <서비스계정_이메일>
    client_id: <클라이언트_ID>
    client_x509_cert_url: <인증서_URL>
    private_key: <프라이빗_키>
    private_key_id: <키_ID>
    project_id: <GCP_프로젝트_ID>
    token_uri: https://oauth2.googleapis.com/token
    type: service_account
    universe_domain: googleapis.com
metadata:
  currency: USD  # 통화 단위 (USD, KRW 등)
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
options:  # 플러그인 실행 옵션
  source: bigquery                          # 데이터 소스: bigquery 또는 gcs
  billing_export_project_id: <프로젝트_ID>   # 빌링 Export가 있는 프로젝트 ID
  billing_dataset_id: <데이터셋_ID>          # BigQuery 데이터셋 ID
  billing_account_id: <빌링_계정_ID>         # GCP 빌링 계정 ID
  select_cost: list_price                   # 비용 선택: list_price 또는 actual_cost
  currency: USD                             # 통화 단위
upgrade_mode: AUTO                          # 자동 업데이트 여부
schedule:
  state: ENABLED                            # 스케줄 활성화
  hours: 16                               # 실행 시간 (0-23)
  resource_group: DOMAIN
tags: {}
```

**등록 명령어:**
```bash
spacectl exec register cost_analysis.DataSource -f register_datasource.yaml
```

## 설정 옵션 상세 설명

### 데이터 소스별 설정

#### BigQuery 소스 사용 시
```yaml
options:
  source: bigquery
  billing_export_project_id: <빌링_Export_프로젝트_ID>
  billing_dataset_id: <BigQuery_데이터셋_ID>
  billing_account_id: <빌링_계정_ID>
  select_cost: list_price  # 또는 actual_cost
  currency: USD
```

#### Google Cloud Storage 소스 사용 시  
```yaml
options:
  source: gcs
  billing_export_project_id: <빌링_Export_프로젝트_ID>
  bucket_name: <GCS_버킷명>
  account_id: <사용자_계정_ID>
  select_cost: list_price  # 또는 actual_cost
  currency: USD
```

### 주요 설정 항목

| 옵션 | 설명 | 필수여부 | 예시 |
|------|------|----------|------|
| `source` | 데이터 소스 선택 | 필수 | `bigquery`, `gcs` |
| `billing_export_project_id` | 빌링 Export 데이터가 저장된 GCP 프로젝트 ID | 필수 | `my-billing-project` |
| `billing_dataset_id` | BigQuery 데이터셋 ID (BigQuery 소스 시) | BigQuery 사용시 필수 | `billing_data` |
| `billing_account_id` | GCP 빌링 계정 ID | 필수 | `01AB23-CD45EF-GH67IJ` |
| `bucket_name` | GCS 버킷명 (GCS 소스 시) | GCS 사용시 필수 | `my-billing-bucket` |
| `select_cost` | 비용 데이터 선택 기준 | 필수 | `list_price`, `actual_cost` |
| `currency` | 통화 단위 | 필수 | `USD`, `KRW` |

## 사전 준비사항

### GCP 설정
1. **Billing Export 활성화**: GCP 콘솔에서 Cloud Billing Export를 BigQuery 또는 GCS로 설정
2. **Service Account 생성**: 다음 권한을 가진 서비스 계정 필요:
   - BigQuery 사용 시: `BigQuery Data Viewer`, `BigQuery Job User`
   - GCS 사용 시: `Storage Object Viewer`
3. **서비스 계정 키 생성**: JSON 형태의 키 파일 생성

### 빌링 Export 테이블 형식
BigQuery 테이블명은 다음 패턴을 따라야 합니다:
```
gcp_billing_export_v1_{billing_account_id}
```

## 문제 해결

### 일반적인 오류
- **인증 오류**: 서비스 계정 키와 권한을 확인하세요
- **테이블을 찾을 수 없음**: 빌링 계정 ID와 Export 설정을 확인하세요
- **데이터가 수집되지 않음**: 빌링 Export가 활성화되어 있고 데이터가 존재하는지 확인하세요