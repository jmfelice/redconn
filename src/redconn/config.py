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
    port: int = 5439
    timeout: int = 30
    ssl: bool = True
    max_retries: int = 3
    retry_delay: int = 5
    # COPY command credentials (optional)
    iam_role: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_region: Optional[str] = None

    @classmethod
    def from_env(cls) -> 'RedshiftConfig':
        """Create a configuration from environment variables.
        Returns:
            RedshiftConfig: A new configuration instance
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            host=os.environ.get('REDSHIFT_HOST', ''),
            username=os.environ.get('REDSHIFT_USERNAME', ''),
            password=os.environ.get('REDSHIFT_PASSWORD', ''),
            database=os.environ.get('REDSHIFT_DATABASE', ''),
            port=int(os.environ.get('REDSHIFT_PORT', '5439')),
            timeout=int(os.environ.get('REDSHIFT_TIMEOUT', '30')),
            ssl=os.environ.get('REDSHIFT_SSL', 'true').lower() == 'true',
            max_retries=int(os.environ.get('REDSHIFT_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('REDSHIFT_RETRY_DELAY', '5')),
            iam_role=os.environ.get('AWS_IAM_ROLE'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
            aws_region=os.environ.get('AWS_REGION')
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