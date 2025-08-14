# Dataset Example

This example demonstrates how to hydrate the data lake with a sample dataset for a specific team.

## Usage

```bash
./deploy.sh -t <team-name> -d <dataset-name> -r <region> [-p <aws-profile>]
```

### Parameters

- `-t <team-name>` (required): Team name (2-12 characters, lowercase letters and numbers only)
- `-d <dataset-name>` (required): Dataset name (2-12 characters, lowercase letters and numbers only)
- `-r <region>` (required): AWS region (e.g., us-east-1, eu-west-1)
- `-p <aws-profile>` (optional): AWS profile to use
- `-h`: Show help message

### Examples

```bash
# Deploy legislators dataset for team "engineering" in us-east-1
./deploy.sh -t engineering -d legislators -r us-east-1

# Deploy customers dataset for team "analytics" in eu-west-1 with specific AWS profile
./deploy.sh -t analytics -d customers -r eu-west-1 -p my-profile
```

## What it creates

1. **Glue Job**: `sdlf-<team-name>-<dataset-name>-glue-job`
2. **IAM Role**: `sdlf-<team-name>-<dataset-name>-glue-role`
3. **CloudFormation Stack**: `sdlf-<team-name>-<dataset-name>-glue-job`
4. **S3 Data**: Uploads sample data to `s3://<raw-bucket>/<team-name>/<dataset-name>/`

## Data Processing

The Glue job processes three JSON files:
- `persons.json` → `persons/` (Parquet)
- `memberships.json` → `memberships/` (Parquet)
- `organizations.json` → `organizations/` (Parquet)
- Creates a joined `history/` dataset partitioned by organization name

## Prerequisites

- AWS CLI configured
- SDLF framework deployed in the specified region
- Appropriate IAM permissions

## Example Resource Names

For team "engineering" and dataset "legislators":
- Glue Job: `sdlf-engineering-legislators-glue-job`
- IAM Role: `sdlf-engineering-legislators-glue-role`
- S3 Path: `s3://<raw-bucket>/engineering/legislators/`
