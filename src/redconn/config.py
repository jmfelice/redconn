from dataclasses import dataclass
import os
from typing import Optional

@dataclass
class RedshiftConfig:
    """Configuration for Redshift connection.
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    """
    host: str
    username: str
    password: str
    database: str
    port: int 
    timeout: int = 30
    ssl: bool = True
    max_retries: int = 3
    retry_delay: int = 5
    
    # COPY command credentials (optional)
    s3_bucket_name: Optional[str] = None
    s3_directory: Optional[str] = None
    redshift_cluster: Optional[str] = None
    aws_iam_role: Optional[str] = None
    aws_region: Optional[str] = None


    @classmethod
    def from_env(cls) -> 'RedshiftConfig':
        """Create a configuration from environment variables.
        Returns:
            RedshiftConfig: A new configuration instance
        Raises:
            ValueError: If required environment variables are missing
        """
        required_vars = [
            'REDSHIFT_HOST',
            'REDSHIFT_USERNAME',
            'REDSHIFT_PASSWORD',
            'REDSHIFT_DATABASE',
            'REDSHIFT_PORT',
        ]
        missing = [var for var in required_vars if not os.environ.get(var)]
        if missing:
            raise ValueError(
                f"Missing required environment variables for RedshiftConfig: {', '.join(missing)}. "
                "Please set these variables or provide them directly."
            )
        return cls(
            # Required
            host=os.environ.get('REDSHIFT_HOST'),
            username=os.environ.get('REDSHIFT_USERNAME'),
            password=os.environ.get('REDSHIFT_PASSWORD'),
            database=os.environ.get('REDSHIFT_DATABASE'),
            port=int(os.environ.get('REDSHIFT_PORT')),
                     
            # Default
            timeout=int(os.environ.get('REDSHIFT_TIMEOUT', '30')),
            ssl=os.environ.get('REDSHIFT_SSL', 'true').lower() == 'true',
            max_retries=int(os.environ.get('REDSHIFT_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('REDSHIFT_RETRY_DELAY', '5')),

            # Optional for COPY command
            s3_bucket_name = os.environ.get('S3_BUCKET_NAME', None),
            s3_directory = os.environ.get('S3_DIRECTORY', None),            
            redshift_cluster = os.environ.get('REDSHIFT_CLUSTER', None),
            aws_iam_role = os.environ.get('AWS_IAM_ROLE', None),
            aws_region = os.environ.get('AWS_REGION', None)
        )

    def validate(self) -> None:
        """Validate the configuration parameters.
        Raises:
            ValueError: If any required parameters are missing or invalid
        """
        if not self.host:
            raise ValueError("Host cannot be empty")
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.password:
            raise ValueError("Password cannot be empty")
        if not self.database:
            raise ValueError("Database cannot be empty")
        if self.port <= 0:
            raise ValueError("Port must be a positive number")
        if self.timeout <= 0:
            raise ValueError("Timeout must be a positive number")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative") 