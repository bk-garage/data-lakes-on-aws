# Datalake Library

A modern, unified Python library for AWS data lake operations. The datalake_library provides clean interfaces for S3, DynamoDB, Step Functions, SQS, and KMS operations with shared boto3 sessions for optimal performance.

## Architecture

The library is built around a unified `DataLakeClient` that provides access to all AWS services through dedicated interfaces:

```
datalake_library/
├── interfaces/                    # Service-specific interfaces
│   ├── base_interface.py         # BaseInterface
│   ├── s3_interface.py           # S3 operations
│   ├── dynamo_interface.py       # DynamoDB operations  
│   ├── states_interface.py       # Step Functions operations
│   ├── sqs_interface.py          # SQS operations
│   └── kms_interface.py          # KMS operations
├── client.py                     # Unified DataLakeClient
├── __init__.py                   # Public API exports
└── commons.py                    # Shared utilities
```

## Installation

The library is packaged as a Lambda Layer and automatically mounted to Lambda functions. No manual installation required.

## Usage

### Basic Usage

```python
from datalake_library import DataLakeClient

# Initialize client with team/pipeline/stage context
client = DataLakeClient(team="engineering", pipeline="stage", stage="a")

# Access all services through the client
client.s3.download_object(bucket, key)
client.dynamo.update_object_metadata_catalog(metadata)
client.states.run_state_machine(arn, payload)
client.sqs.send_message_to_fifo_queue(message, group_id, queue_url)
client.kms.data_kms_key
```

### Individual Interfaces

```python
from datalake_library import S3Interface, DynamoInterface

# Use individual interfaces with shared session
import boto3
session = boto3.Session()

s3 = S3Interface(team="engineering", session=session)
dynamo = DynamoInterface(team="engineering", session=session)
```

## API Reference

### DataLakeClient

```python
client = DataLakeClient(
    team="engineering",      # Team name for configuration lookup
    dataset="legislators",   # Dataset name (optional)
    pipeline="stage",        # Pipeline name
    stage="a",              # Stage name
    log_level="INFO",       # Logging level (optional)
    session=None            # Boto3 session (optional)
)
```

### S3Interface

```python
# Download object to /tmp
local_path = client.s3.download_object(bucket, key)

# Upload object with KMS encryption
client.s3.upload_object(local_path, bucket, key, kms_key=kms_key)

# Copy object between buckets
client.s3.copy_object(src_bucket, src_key, dest_bucket, dest_key)

# Get object metadata
size, last_modified = client.s3.get_size_and_last_modified(bucket, key)

# Access bucket names
client.s3.raw_bucket
client.s3.stage_bucket
client.s3.analytics_bucket
```

### DynamoInterface

```python
# Update object metadata catalog
metadata = {
    "bucket": bucket,
    "key": key,
    "team": team,
    "dataset": dataset
}
client.dynamo.update_object_metadata_catalog(metadata)

# Put item to any table
client.dynamo.put_item(table_name, item)

# Access table names
client.dynamo.object_metadata_table
client.dynamo.manifests_table
```

### StatesInterface

```python
# Run state machine
response = client.states.run_state_machine(arn, payload)

# Access state machine ARN
client.states.state_machine_arn
```

### SQSInterface

```python
# Send message to FIFO queue
client.sqs.send_message_to_fifo_queue(message, group_id, queue_url)

# Receive messages
messages = client.sqs.receive_messages(max_messages, queue_url)

# Access queue URLs
client.sqs.stage_queue_url
client.sqs.stage_dlq_url
```

### KMSInterface

```python
# Access KMS key ARN
kms_key = client.kms.data_kms_key
```

## Pipeline

The library is automatically packaged into a Lambda Layer via CodeBuild when changes are committed to the environment branch (`dev`, `test`, or `main`). The layer is then made available to all Lambda functions in the data lake pipeline.

**Size Limits:**
- Zipped: 50MB maximum
- Unzipped: 250MB maximum
