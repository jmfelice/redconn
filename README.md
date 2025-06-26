# redconn: Redshift Connector

A Pythonic interface for Amazon Redshift, supporting environment-based configuration, connection management, query execution, and data loading.

## Installation

```bash
pip install redconn
```

## Environment Variables

You can configure RedConn using environment variables (recommended for security):

- `REDSHIFT_HOST` (required)
- `REDSHIFT_USERNAME` (required)
- `REDSHIFT_PASSWORD` (required)
- `REDSHIFT_DATABASE` (required)
- `REDSHIFT_PORT` (required)

- `REDSHIFT_TIMEOUT` (default: 30)
- `REDSHIFT_SSL` (default: true)
- `REDSHIFT_MAX_RETRIES` (default: 3)
- `REDSHIFT_RETRY_DELAY` (default: 5)

- `S3_BUCKET_NAME` (optional, for COPY)
- `S3_DIRECTORY` (optional, for COPY)
- `REDSHIFT_CLUSTER` (optional, for COPY)
- `AWS_IAM_ROLE` (optional, for COPY)
- `AWS_REGION` (optional, for COPY)

Example (Linux/macOS):
```bash
export REDSHIFT_HOST=mycluster.abc123.us-west-2.redshift.amazonaws.com
export REDSHIFT_USERNAME=myuser
export REDSHIFT_PASSWORD=mypassword
export REDSHIFT_DATABASE=mydb
export REDSHIFT_PORT=5439
export REDSHIFT_TIMEOUT=30
export REDSHIFT_SSL=true
export REDSHIFT_MAX_RETRIES=3
export REDSHIFT_RETRY_DELAY=5
export S3_BUCKET_NAME=my-bucket
export S3_DIRECTORY=data/
export REDSHIFT_CLUSTER=my-redshift-cluster
export AWS_IAM_ROLE=arn:aws:iam::123456789012:role/MyRedshiftRole
export AWS_REGION=us-west-2
```

## Quickstart

### 1. Connecting to Redshift

```python
from redconn import RedConn

# Uses environment variables by default
conn = RedConn()
conn.connect()

# Or use explicit config
from redconn import RedshiftConfig
config = RedshiftConfig(
    host="mycluster.abc123.us-west-2.redshift.amazonaws.com",
    username="myuser",
    password="mypassword",
    database="mydb"
)
conn = RedConn(config=config)
conn.connect()
```

### 2. Querying Data

```python
# Fetch all results as a DataFrame
import pandas as pd
df = conn.fetch("SELECT * FROM my_table LIMIT 10")

# Fetch in chunks (generator of DataFrames)
for chunk in conn.fetch("SELECT * FROM my_table", chunksize=1000):
    print(chunk)
```

### 3. Executing Statements

```python
# Single statement
results = conn.execute_statements("CREATE TABLE test (id INT)")
print(results)

# Multiple statements (sequential)
statements = [
    "INSERT INTO test VALUES (1)",
    "INSERT INTO test VALUES (2)"
]
results = conn.execute_statements(statements)
print(results)

# Multiple statements (parallel)
results = conn.execute_statements(statements, parallel=True)
print(results)
```

### 4. COPY from S3

```python
# Build a COPY statement for loading CSV from S3
copy_sql = conn.build_copy_statement(
    table="my_table",
    s3_path="s3://my-bucket/data.csv",
    format="CSV",
    delimiter=",",
    ignoreheader=1,
    iam_role="arn:aws:iam::123456789012:role/MyRedshiftRole",
    aws_region="us-west-2"
)
print(copy_sql)

# Execute the COPY
conn.execute_statements(copy_sql)
```

### 5. Context Manager Usage

```python
from redconn import RedConn
with RedConn() as conn:
    df = conn.fetch("SELECT 1")
    print(df)
```

## Error Handling

- `ConnectionError`: Raised for connection issues
- `QueryError`: Raised for query execution issues
- `AuthenticationError`: Raised for authentication failures

## License

See [LICENSE](LICENSE).
