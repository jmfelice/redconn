import pandas as pd
import redshift_connector
from redshift_connector import Connection
from typing import Union, List, Dict, Optional, Any, Generator
import time
from concurrent.futures import ThreadPoolExecutor
import logging
from .config import RedshiftConfig
from .utils import _sanitize_log_message, _sanitize_query
from .exceptions import ConnectionError, QueryError

# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class RedConn:
    """
    A class to handle Redshift database connections and operations.
    Implements context manager protocol for safe resource management.

    This class supports:
      - Configuration via environment variables or explicit parameters
      - Connection management with retry logic
      - Querying data as pandas DataFrames (with chunking support)
      - Executing single or multiple SQL statements (sequentially or in parallel)
      - Building and executing Redshift COPY statements from S3

    **Environment Variables:**
        - REDSHIFT_HOST, REDSHIFT_USERNAME, REDSHIFT_PASSWORD, REDSHIFT_DATABASE (required)
        - REDSHIFT_PORT, REDSHIFT_TIMEOUT, REDSHIFT_SSL, REDSHIFT_MAX_RETRIES, REDSHIFT_RETRY_DELAY (optional)
        - AWS_IAM_ROLE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION (for COPY)

    **Examples:**

    .. code-block:: python

        from redconn import RedConn

        # 1. Connect using environment variables
        conn = RedConn()
        conn.connect()

        # 2. Query data
        df = conn.fetch("SELECT * FROM my_table LIMIT 10")

        # 3. Execute statements
        results = conn.execute_statements([
            "CREATE TABLE test (id INT)",
            "INSERT INTO test VALUES (1)"
        ])

        # 4. COPY from S3
        copy_sql = conn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv",
            format="CSV",
            delimiter=",",
            ignoreheader=1,
            iam_role="arn:aws:iam::123456789012:role/MyRedshiftRole",
            aws_region="us-west-2"
        )
        conn.execute_statements(copy_sql)

        # 5. Context manager usage
        with RedConn() as conn:
            df = conn.fetch("SELECT 1")
            print(df)

    **Testing:**
        For testing purposes, you can override the following methods:
        - _get_connection(): Override to return a mock connection
        - _get_cursor(): Override to return a mock cursor

        Example:

        .. code-block:: python

            class MockRedConn(RedConn):
                def _get_connection(self):
                    return MockConnection()
    """
    
    def __init__(
        self,
        config: Optional[RedshiftConfig] = None,
        **kwargs
    ):
        """
        Initialize the RedConn class with a RedshiftConfig instance or keyword arguments.

        Args:
            config (Optional[RedshiftConfig]): An instance of RedshiftConfig to use directly
            **kwargs: Arguments to build a RedshiftConfig if config is not provided
        """
        if config is not None:
            self.config = config
        else:
            # Use environment variables if present, otherwise use provided/default values
            env_config = RedshiftConfig.from_env()
            self.config = RedshiftConfig(
                host=kwargs.get('host', env_config.host or ""),
                username=kwargs.get('username', env_config.username or ""),
                password=kwargs.get('password', env_config.password or ""),
                database=kwargs.get('database', env_config.database or ""),
                port=kwargs.get('port', env_config.port),
                timeout=kwargs.get('timeout', env_config.timeout),
                ssl=kwargs.get('ssl', env_config.ssl),
                max_retries=kwargs.get('max_retries', env_config.max_retries),
                retry_delay=kwargs.get('retry_delay', env_config.retry_delay),
                iam_role=kwargs.get('iam_role', env_config.iam_role),
                aws_access_key_id=kwargs.get('aws_access_key_id', env_config.aws_access_key_id),
                aws_secret_access_key=kwargs.get('aws_secret_access_key', env_config.aws_secret_access_key),
                aws_session_token=kwargs.get('aws_session_token', env_config.aws_session_token),
                aws_region=kwargs.get('aws_region', env_config.aws_region)
            )
        self.conn: Optional[Connection] = None
        self.echo: bool = False
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the configuration parameters"""
        self.config.validate()

    def _get_connection(self) -> Connection:
        """Get a database connection. Override this method for testing.
        
        Returns:
            Connection: A database connection instance
            
        Raises:
            ConnectionError: If there's an error establishing the connection
        """
        return redshift_connector.connect(
            host=self.config.host,
            user=self.config.username,
            password=self.config.password,
            database=self.config.database,
            port=self.config.port,
            timeout=self.config.timeout,
            ssl=self.config.ssl
        )
    
    def _get_cursor(self) -> redshift_connector.Cursor:
        """Get a database cursor. Override this method for testing.
        
        Returns:
            redshift_connector.Cursor: A database cursor instance
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn.cursor()
    
    @property
    def connection(self) -> Connection:
        """Get the current database connection.
        
        Returns:
            Connection: The current database connection
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn
    
    @property
    def cursor(self) -> redshift_connector.Cursor:
        """Get the current database cursor.
        
        Returns:
            redshift_connector.Cursor: The current database cursor
            
        Raises:
            ConnectionError: If there's no active connection
        """
        return self._get_cursor()

    def connect(self) -> Connection:
        """
        Establishes a connection to a Redshift database using credentials.
        Implements retry logic for transient failures.

        Returns:
            Connection: A connection object to the Redshift database

        Raises:
            ConnectionError: If there's an error establishing the connection after retries
        """
        for attempt in range(self.config.max_retries):
            try:
                self.conn = self._get_connection()
                return self.conn
            except Exception as e:
                error_msg = _sanitize_log_message(str(e))
                if attempt == self.config.max_retries - 1:
                    raise ConnectionError(f"Failed to connect after {self.config.max_retries} attempts: {error_msg}")
                logger.warning(f"Connection attempt {attempt + 1} failed: {error_msg}")
                time.sleep(self.config.retry_delay)

    def __enter__(self) -> 'RedConn':
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()

    def close(self) -> None:
        """
        Closes the database connection if it exists.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as e:
                error_msg = _sanitize_log_message(str(e))
                logger.error(f"Error closing connection: {error_msg}")
            finally:
                self.conn = None

    def fetch(
        self,
        query: str,
        echo: Optional[bool] = None,
        chunksize: Optional[int] = None
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Executes a SQL query using the connection and returns the results.

        Args:
            query (str): The SQL query to be executed
            echo (Optional[bool]): Whether to print the query before execution
            chunksize (Optional[int]): Size of chunks to read data in. If None, reads all data at once.
                                     When specified, returns a generator of DataFrames.

        Returns:
            Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]: 
                If chunksize is None, returns the result as a DataFrame.
                If chunksize is specified, returns a generator of DataFrames.

        Raises:
            QueryError: If there's an error executing the query
            ConnectionError: If there's an error with the database connection
        """
        if self.conn is None:
            self.connect()

        query = query.replace(";", "")
        echo = echo if echo is not None else self.echo

        if echo:
            # Sanitize query before logging
            sanitized_query = _sanitize_query(query)
            logger.info(f"Executing query: {sanitized_query}")

        try:
            if chunksize:
                return pd.read_sql(sql=query, con=self.conn, chunksize=chunksize)
            else:
                return pd.read_sql(sql=query, con=self.conn)
        
        except Exception as e:
            # Sanitize error message before logging
            error_msg = _sanitize_log_message(str(e))
            raise QueryError(f"Error executing query: {error_msg}")

    def execute_statements(
        self, 
        statements: Union[str, List[str]], 
        parallel: bool = False,
        echo: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes one or more SQL statements in Redshift and returns the results.

        Args:
            statements (Union[str, List[str]]): A single SQL statement or list of SQL statements
            parallel (bool): If True, executes statements in parallel using separate connections.
                           WARNING: Parallel execution creates separate connections which may
                           increase security risk. Use with caution.
            echo (Optional[bool]): Whether to print the query before execution

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing execution results.
            Each dictionary has the following keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        if isinstance(statements, str):
            statements = [statements]

        start_time = time.time()
        echo = echo if echo is not None else self.echo

        if parallel:
            logger.warning("Using parallel execution mode. This creates separate database connections and may increase security risk.")
            with ThreadPoolExecutor(max_workers=len(statements)) as executor:
                results = list(executor.map(self._execute_single_statement, 
                                         [_sanitize_query(stmt) for stmt in statements]))
            return results

        # Sequential execution
        if self.conn is None:
            self.connect()

        results = []
        for statement in statements:
            statement_start_time = time.time()
            with self.conn.cursor() as connection:
                try:
                    connection.execute(statement)
                    self.conn.commit()  # Explicitly commit the transaction
                    duration = time.time() - statement_start_time
                    results.append({
                        "success": True, 
                        "statement": _sanitize_query(statement),
                        "duration": duration
                    })
                    if echo:
                        logger.info(f"Success: Statement = {_sanitize_query(statement)}, Duration = {duration} seconds")
                except Exception as e:
                    duration = time.time() - statement_start_time
                    error_msg = _sanitize_log_message(str(e))
                    results.append({
                        "success": False, 
                        "statement": _sanitize_query(statement), 
                        "error": error_msg,
                        "duration": duration
                    })
                    if echo:
                        logger.error(f"Failed: Statement = {_sanitize_query(statement)}, Duration = {duration} seconds")

        total_duration = time.time() - start_time
        if echo:
            logger.info(f"Total time taken to execute {len(statements)} statements: {total_duration} seconds")
        return results

    def _execute_single_statement(self, stmt: str) -> Dict[str, Any]:
        """
        Execute a single statement in its own connection.

        Args:
            stmt (str): The SQL statement to execute

        Returns:
            Dict[str, Any]: Dictionary containing execution results with keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        statement_start_time = time.time()
        
        # Create a new connection for this thread
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(stmt)
            conn.commit()
            duration = time.time() - statement_start_time
            result = {
                "statement": _sanitize_query(stmt),
                "success": True,
                "duration": duration
            }
            if self.echo:
                logger.info(f"Success: Statement = {_sanitize_query(stmt)}, Duration = {duration} seconds")
            return result

        except Exception as e:
            duration = time.time() - statement_start_time
            error_msg = _sanitize_log_message(str(e))
            result = {
                "statement": _sanitize_query(stmt),
                "success": False,
                "error": error_msg,
                "duration": duration
            }
            if self.echo:
                logger.error(f"Failed: Statement = {_sanitize_query(stmt)}, Duration = {duration} seconds")
            return result
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def build_copy_statement(
        self,
        table: str,
        s3_path: str,
        format: str = 'CSV',
        delimiter: str = ',',
        ignoreheader: int = 1,
        gzip: bool = False,
        manifest: bool = False,
        timeformat: str = 'YYYY-MM-DD-HH.MI.SS',
        dateformat: str = 'YYYY-MM-DD',
        additional_options: Optional[dict] = None
    ) -> str:
        """
        Build a Redshift COPY statement with sensible defaults for CSV loading.
        Uses credentials and region from self.config.

        Args:
            table (str): Target table name
            s3_path (str): S3 URI to load data from
            format (str): Data format (default 'CSV')
            delimiter (str): Field delimiter (default ',')
            ignoreheader (int): Number of header rows to skip (default 1)
            gzip (bool): Whether the file is gzipped (default False)
            manifest (bool): Whether the S3 path is a manifest file (default False)
            timeformat (str): Time format for COPY (default 'YYYY-MM-DD-HH.MI.SS')
            dateformat (str): Date format for COPY (default 'YYYY-MM-DD')
            additional_options (Optional[dict]): Additional COPY options as key-value pairs

        Returns:
            str: The constructed COPY statement
        """
        def esc(val: str) -> str:
            return f"'{val.replace(chr(39), chr(39)+chr(39))}'"

        config = self.config
        region = getattr(config, 'aws_region', None)
        iam_role = getattr(config, 'iam_role', None)
        access_key_id = getattr(config, 'aws_access_key_id', None)
        secret_access_key = getattr(config, 'aws_secret_access_key', None)
        session_token = getattr(config, 'aws_session_token', None)

        opts_dict = dict(additional_options) if additional_options else {}
        # Always include quote, timeformat, dateformat unless explicitly set
        if not any(k.lower() == 'quote' for k in opts_dict):
            opts_dict['QUOTE'] = '"'
        if not any(k.lower() == 'timeformat' for k in opts_dict):
            opts_dict['TIMEFORMAT'] = timeformat
        if not any(k.lower() == 'dateformat' for k in opts_dict):
            opts_dict['DATEFORMAT'] = dateformat

        creds = []
        if iam_role:
            creds.append(f"iam_role {esc(iam_role)}")
        elif access_key_id and secret_access_key:
            creds.append(f"access_key_id {esc(access_key_id)} secret_access_key {esc(secret_access_key)}")
            if session_token:
                creds.append(f"session_token {esc(session_token)}")
        else:
            raise ValueError("Either iam_role or access_key_id/secret_access_key must be provided in RedshiftConfig.")

        opts = []
        if region:
            opts.append(f"region {esc(region)}")
        if format.upper() == 'CSV':
            opts.append("format as csv")
            if delimiter:
                opts.append(f"delimiter {esc(delimiter)}")
            if ignoreheader is not None:
                opts.append(f"ignoreheader {ignoreheader}")
        elif format.upper() == 'JSON':
            opts.append("format as json 'auto'")
        else:
            opts.append(f"format as {format.lower()}")
        if gzip:
            opts.append("gzip")
        if manifest:
            opts.append("manifest")
        for k, v in opts_dict.items():
            if isinstance(v, bool):
                if v:
                    opts.append(k)
            else:
                opts.append(f"{k} {esc(str(v))}")

        statement = f"COPY {table} FROM {esc(s3_path)} {' '.join(creds + opts)};"
        return statement
