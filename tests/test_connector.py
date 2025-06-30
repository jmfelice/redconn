"""Tests for RedConn class."""

import pytest
import pandas as pd
import time
from unittest.mock import Mock, MagicMock, patch, call
from typing import Generator

from redconn.connector import RedConn
from redconn.config import RedshiftConfig
from redconn.exceptions import ConnectionError, QueryError


class MockConnection:
    """Mock connection for testing."""
    
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.closed = False
        self._cursor = MockCursor(should_fail=should_fail)
    
    def cursor(self):
        """Return a mock cursor."""
        return self._cursor
    
    def close(self):
        """Mock close method."""
        if self.should_fail:
            raise Exception("Failed to close connection")
        self.closed = True
    
    def commit(self):
        """Mock commit method."""
        if self.should_fail:
            raise Exception("Failed to commit")

class MockCursor:
    """Mock cursor for testing."""
    
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.executed_queries = []
        self.closed = False
    
    def execute(self, query):
        """Mock execute method."""
        self.executed_queries.append(query)
        if self.should_fail:
            raise Exception("Query execution failed")
    
    def close(self):
        """Mock close method."""
        self.closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class MockableRedConn(RedConn):
    """RedConn subclass for testing with overridable methods."""
    
    def __init__(self, mock_connection=None, mock_cursor=None, **kwargs):
        self._mock_connection = mock_connection
        self._mock_cursor = mock_cursor
        super().__init__(**kwargs)
    
    def _get_connection(self):
        """Override to return mock connection."""
        if self._mock_connection:
            return self._mock_connection
        return MockConnection()
    
    def _get_cursor(self):
        """Override to return mock cursor."""
        if self._mock_cursor:
            return self._mock_cursor
        return super()._get_cursor()

class TestRedConnInitialization:
    """Test RedConn initialization."""
    
    def test_init_with_config_object(self):
        """Test initialization with RedshiftConfig object."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            port=5439
        )
        redconn = MockableRedConn(config=config)
        
        assert redconn.config == config
        assert redconn.conn is None
        assert redconn.echo is False
    
    def test_init_with_kwargs(self):
        """Test initialization with keyword arguments."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            port=6789
        )
        
        assert redconn.config.host == "test-host"
        assert redconn.config.username == "test-user"
        assert redconn.config.password == "test-pass"
        assert redconn.config.database == "test-db"
        assert redconn.config.port == 6789
    
    @patch.dict('os.environ', {
        'REDSHIFT_HOST': 'env-host',
        'REDSHIFT_USERNAME': 'env-user',
        'REDSHIFT_PASSWORD': 'env-pass',
        'REDSHIFT_DATABASE': 'env-db'
    })
    def test_init_with_env_vars(self):
        """Test initialization uses environment variables when no config provided."""
        redconn = MockableRedConn()
        
        assert redconn.config.host == "env-host"
        assert redconn.config.username == "env-user"
        assert redconn.config.password == "env-pass"
        assert redconn.config.database == "env-db"
    
    def test_init_validates_config(self):
        """Test that initialization validates the config."""
        with pytest.raises(ValueError, match="Host cannot be empty"):
            MockableRedConn(
                host="",
                username="test-user",
                password="test-pass",
                database="test-db"
            )

class TestRedConnConnection:
    """Test RedConn connection methods."""
    
    def test_connect_success(self):
        """Test successful connection."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        result = redconn.connect()
        
        assert result == mock_conn
        assert redconn.conn == mock_conn
    
    def test_connect_with_retries(self):
        """Test connection with retry logic."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            max_retries=3,
            retry_delay=0.1
        )
        
        # Mock _get_connection to fail twice then succeed
        call_count = 0
        def mock_get_connection():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("Connection failed")
            return MockConnection()
        
        redconn._get_connection = mock_get_connection
        
        with patch('time.sleep'):  # Speed up test
            result = redconn.connect()
        
        assert isinstance(result, MockConnection)
        assert call_count == 3
    
    def test_connect_max_retries_exceeded(self):
        """Test connection failure after max retries."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            max_retries=2,
            retry_delay=0.1
        )
        
        # Mock _get_connection to always fail
        def mock_get_connection():
            raise Exception("Connection failed")
        
        redconn._get_connection = mock_get_connection
        
        with patch('time.sleep'):  # Speed up test
            with pytest.raises(ConnectionError, match="Failed to connect after 2 attempts"):
                redconn.connect()
    
    def test_close_connection(self):
        """Test closing connection."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn.connect()
        redconn.close()
        
        assert mock_conn.closed is True
        assert redconn.conn is None
    
    def test_close_connection_with_error(self):
        """Test closing connection that raises an error."""
        mock_conn = MockConnection(should_fail=True)
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn.connect()
        
        # Should not raise exception but log error
        with patch('redconn.connector.logger') as mock_logger:
            redconn.close()
            mock_logger.error.assert_called_once()
        
        assert redconn.conn is None
    
    def test_context_manager(self):
        """Test using RedConn as a context manager."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with redconn as conn:
            assert conn == redconn
            assert redconn.conn == mock_conn
        
        assert mock_conn.closed is True
        assert redconn.conn is None

class TestRedConnProperties:
    """Test RedConn properties."""
    
    def test_connection_property_success(self):
        """Test connection property when connection exists."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn.connect()
        assert redconn.connection == mock_conn
    
    def test_connection_property_no_connection(self):
        """Test connection property when no connection exists."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with pytest.raises(ConnectionError, match="No active database connection"):
            _ = redconn.connection
    
    def test_cursor_property_success(self):
        """Test cursor property when connection exists."""
        mock_cursor = MockCursor()
        mock_conn = MockConnection()
        mock_conn._cursor = mock_cursor
        
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn.connect()
        assert redconn.cursor == mock_cursor
    
    def test_cursor_property_no_connection(self):
        """Test cursor property when no connection exists."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with pytest.raises(ConnectionError, match="No active database connection"):
            _ = redconn.cursor

class TestRedConnFetch:
    """Test RedConn fetch method."""
    
    @patch('pandas.read_sql')
    def test_fetch_success(self, mock_read_sql):
        """Test successful query fetch."""
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        mock_read_sql.return_value = mock_df
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        result = redconn.fetch("SELECT * FROM test_table")
        
        assert result.equals(mock_df)
        mock_read_sql.assert_called_once_with(sql="SELECT * FROM test_table", con=mock_conn)
    
    @patch('pandas.read_sql')
    def test_fetch_with_chunksize(self, mock_read_sql):
        """Test fetch with chunksize parameter."""
        mock_generator = (pd.DataFrame({'col1': [i]}) for i in range(3))
        mock_read_sql.return_value = mock_generator
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        result = redconn.fetch("SELECT * FROM test_table", chunksize=100)
        
        assert isinstance(result, Generator)
        mock_read_sql.assert_called_once_with(sql="SELECT * FROM test_table", con=mock_conn, chunksize=100)
    
    @patch('pandas.read_sql')
    def test_fetch_auto_connect(self, mock_read_sql):
        """Test that fetch auto-connects if no connection exists."""
        mock_df = pd.DataFrame({'col1': [1]})
        mock_read_sql.return_value = mock_df
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        # No explicit connect call
        result = redconn.fetch("SELECT * FROM test_table")
        
        assert redconn.conn == mock_conn
        assert result.equals(mock_df)
    
    @patch('pandas.read_sql')
    def test_fetch_strips_semicolon(self, mock_read_sql):
        """Test that fetch strips semicolons from queries."""
        mock_df = pd.DataFrame({'col1': [1]})
        mock_read_sql.return_value = mock_df
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn.fetch("SELECT * FROM test_table;")
        
        mock_read_sql.assert_called_once_with(sql="SELECT * FROM test_table", con=mock_conn)
    
    @patch('pandas.read_sql')
    def test_fetch_with_echo(self, mock_read_sql):
        """Test fetch with echo enabled."""
        mock_df = pd.DataFrame({'col1': [1]})
        mock_read_sql.return_value = mock_df
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with patch('redconn.connector.logger') as mock_logger:
            redconn.fetch("SELECT * FROM test_table", echo=True)
            mock_logger.info.assert_called_once_with("Executing query: SELECT * FROM test_table")
    
    @patch('pandas.read_sql')
    def test_fetch_query_error(self, mock_read_sql):
        """Test fetch raises QueryError on database error."""
        mock_read_sql.side_effect = Exception("Database error")
        
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with pytest.raises(QueryError, match="Error executing query"):
            redconn.fetch("SELECT * FROM test_table")

class TestRedConnExecuteStatements:
    """Test RedConn execute_statements method."""
    
    def test_execute_single_statement_success(self):
        """Test executing a single statement successfully."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with patch('time.time', side_effect=[0, 1, 2, 3]):  # Mock timing: start, stmt_start, stmt_end, total_end
            results = redconn.execute_statements("CREATE TABLE test (id INT)")
        
        assert len(results) == 1
        assert results[0]['success'] is True
        assert results[0]['statement'] == "CREATE TABLE test (id INT)"
        assert results[0]['duration'] == 1
        assert 'error' not in results[0]
    
    def test_execute_multiple_statements_success(self):
        """Test executing multiple statements successfully."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        statements = [
            "CREATE TABLE test1 (id INT)",
            "CREATE TABLE test2 (id INT)"
        ]
        
        with patch('time.time', side_effect=[0, 1, 2, 3, 4, 5]):  # Mock timing: start, stmt1_start, stmt1_end, stmt2_start, stmt2_end, total_end
            results = redconn.execute_statements(statements)
        
        assert len(results) == 2
        assert all(r['success'] for r in results)
        assert results[0]['statement'] == "CREATE TABLE test1 (id INT)"
        assert results[1]['statement'] == "CREATE TABLE test2 (id INT)"
    
    def test_execute_statement_with_error(self):
        """Test executing statement that fails."""
        mock_conn = MockConnection(should_fail=True)
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with patch('time.time', side_effect=[0, 1, 2, 3]):  # Mock timing: start, stmt_start, stmt_end, total_end
            results = redconn.execute_statements("INVALID SQL")
        
        assert len(results) == 1
        assert results[0]['success'] is False
        assert results[0]['statement'] == "INVALID SQL"
        assert results[0]['duration'] == 1
        assert 'error' in results[0]
    
    def test_execute_statements_with_echo(self):
        """Test execute_statements with echo enabled."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with patch('redconn.connector.logger') as mock_logger:
            with patch('time.time', side_effect=[0, 1, 2, 3]):  # Mock timing: start, stmt_start, stmt_end, total_end
                redconn.execute_statements("CREATE TABLE test (id INT)", echo=True)
            
            # Check that success and total time were logged
            assert mock_logger.info.call_count >= 1
    
    @patch('redconn.connector.ThreadPoolExecutor')
    def test_execute_statements_parallel(self, mock_executor):
        """Test executing statements in parallel."""
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.map.return_value = [
            {'success': True, 'statement': 'stmt1', 'duration': 1},
            {'success': True, 'statement': 'stmt2', 'duration': 1}
        ]
        
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        statements = ["CREATE TABLE test1 (id INT)", "CREATE TABLE test2 (id INT)"]
        
        with patch('redconn.connector.logger') as mock_logger:
            results = redconn.execute_statements(statements, parallel=True)
        
        assert len(results) == 2
        mock_logger.warning.assert_called_once_with(
            "Using parallel execution mode. This creates separate database connections and may increase security risk."
        )
    
    def test_execute_single_statement_method(self):
        """Test _execute_single_statement method."""
        mock_conn = MockConnection()
        redconn = MockableRedConn(
            mock_connection=mock_conn,
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        with patch('time.time', side_effect=[0, 1]):
            result = redconn._execute_single_statement("CREATE TABLE test (id INT)")
        
        assert result['success'] is True
        assert result['statement'] == "CREATE TABLE test (id INT)"
        assert result['duration'] == 1
        assert 'error' not in result
    
    def test_execute_single_statement_with_error(self):
        """Test _execute_single_statement with database error."""
        redconn = MockableRedConn(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        # Mock _get_connection to return failing connection that can still be closed
        def mock_get_connection():
            mock_conn = MockConnection(should_fail=True)
            # Override close to not fail for this test
            mock_conn.close = Mock()
            return mock_conn
        
        redconn._get_connection = mock_get_connection
        
        with patch('time.time', side_effect=[0, 1]):
            result = redconn._execute_single_statement("INVALID SQL")
        
        assert result['success'] is False
        assert result['statement'] == "INVALID SQL"
        assert result['duration'] == 1
        assert 'error' in result

class TestRedConnBuildCopyStatement:
    """Test RedConn build_copy_statement method."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.redconn = MockableRedConn(
            host="test-host",
            username="test-user", 
            password="test-pass",
            database="test-db",
            s3_bucket_name="test-bucket",
            s3_directory="test-dir",
            aws_iam_role="123456789012",
            aws_region="us-west-2",
            redshift_cluster="test-cluster"
        )
    
    def test_build_copy_statement_basic(self):
        """Test basic COPY statement construction with minimal parameters."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv"
        )
        
        expected_lines = [
            "COPY test-db.public.test_table",
            "FROM 's3://test-bucket/test-dir/data.csv'",
            "IAM_ROLE 'arn:aws:iam::123456789012:role/us-west-2-123456789012-test-cluster'",
            "FORMAT AS",
            "CSV",
            "DELIMITER ','",
            "QUOTE '\"'",
            "IGNOREHEADER 1",
            "REGION AS 'us-west-2'",
            "TIMEFORMAT 'YYYY-MM-DD-HH.MI.SS'",
            "DATEFORMAT as 'YYYY-MM-DD'"
        ]
        
        assert result == '\n'.join(expected_lines)
    
    def test_build_copy_statement_with_full_arn(self):
        """Test COPY statement with full IAM role ARN."""
        full_arn = "arn:aws:iam::123456789012:role/MyRedshiftRole"
        
        result = self.redconn.build_copy_statement(
            redshift_schema_name="analytics",
            redshift_table_name="user_data",
            source_file="users.csv",
            aws_iam_role=full_arn
        )
        
        assert f"IAM_ROLE '{full_arn}'" in result
        assert "COPY test-db.analytics.user_data" in result
        assert "FROM 's3://test-bucket/test-dir/users.csv'" in result
    
    def test_build_copy_statement_override_defaults(self):
        """Test COPY statement with custom parameters overriding defaults."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="staging",
            redshift_table_name="orders",
            source_file="orders.json",
            format="JSON",
            delimiter="|",
            quote="'",
            ignoreheader=2,
            timeformat="YYYY-MM-DD HH24:MI:SS",
            dateformat="MM/DD/YYYY"
        )
        
        assert "FORMAT AS\nJSON" in result
        assert "DELIMITER '|'" in result
        assert "QUOTE \"'\"" in result
        assert "IGNOREHEADER 2" in result
        assert "TIMEFORMAT 'YYYY-MM-DD HH24:MI:SS'" in result
        assert "DATEFORMAT as 'MM/DD/YYYY'" in result
    
    def test_build_copy_statement_custom_s3_path(self):
        """Test COPY statement with custom S3 bucket and directory."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="products",
            source_file="products.csv",
            s3_bucket_name="custom-bucket",
            s3_directory="custom/path"
        )
        
        assert "FROM 's3://custom-bucket/custom/path/products.csv'" in result
    
    def test_build_copy_statement_no_directory(self):
        """Test COPY statement with no S3 directory."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            s3_directory=""
        )
        assert "FROM 's3://test-bucket/data.csv'" in result
    
    def test_build_copy_statement_directory_without_slash(self):
        """Test COPY statement adds slash to directory if missing."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            s3_directory="no-slash-dir"
        )
        
        assert "FROM 's3://test-bucket/no-slash-dir/data.csv'" in result
    
    def test_build_copy_statement_custom_database(self):
        """Test COPY statement with custom database name."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table", 
            source_file="data.csv",
            redshift_database="custom_db"
        )
        
        assert "COPY custom_db.public.test_table" in result
    
    def test_build_copy_statement_boolean_options(self):
        """Test COPY statement with boolean options."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            removequotes=True,
            escape=False,
            emptyasnull=True
        )
        
        assert "REMOVEQUOTES" in result
        assert "ESCAPE" not in result  # False boolean values are not included
        assert "EMPTYASNULL" in result
    
    def test_build_copy_statement_integer_options(self):
        """Test COPY statement with integer options."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            maxerror=100,
            fillrecord=1
        )
        
        assert "MAXERROR 100" in result
        assert "FILLRECORD 1" in result
    
    def test_build_copy_statement_string_options(self):
        """Test COPY statement with custom string options."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            null_as="\\N",
            encoding="UTF8"
        )
        
        assert "NULL_AS '\\N'" in result
        assert "ENCODING 'UTF8'" in result
    
    def test_build_copy_statement_empty_values_ignored(self):
        """Test that empty string and None values are ignored in options."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            null_as="",  # Empty string should be ignored
            encoding=None,  # None should be ignored
            custom_option="valid_value"
        )
        
        assert "NULL_AS" not in result
        assert "ENCODING" not in result
        assert "CUSTOM_OPTION 'valid_value'" in result
    
    def test_build_copy_statement_config_fallback(self):
        """Test that config values are used when not provided in kwargs."""
        # Test fallback to config values
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv"
            # Not providing s3_bucket_name, aws_region etc. - should use config values
        )
        
        assert "s3://test-bucket/test-dir/data.csv" in result
        assert "REGION AS 'us-west-2'" in result
    
    def test_build_copy_statement_json_format(self):
        """Test COPY statement with JSON format and specific options."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="events",
            redshift_table_name="user_events",
            source_file="events.json",
            format="JSON",
            jsonpath="s3://test-bucket/jsonpath.json",
            gzip=True
        )
        
        assert "FORMAT AS\nJSON" in result
        assert "JSONPATH 's3://test-bucket/jsonpath.json'" in result
        assert "GZIP" in result
    
    def test_build_copy_statement_manifest_file(self):
        """Test COPY statement with manifest file option."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="manifest.json",
            manifest=True,
            format="CSV"
        )
        
        assert "MANIFEST" in result
        assert "FORMAT AS\nCSV" in result
    
    def test_build_copy_statement_all_special_formats(self):
        """Test all special format handling cases."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public", 
            redshift_table_name="test_table",
            source_file="data.csv",
            format="PARQUET",
            delimiter="\t",
            quote="'",
            region="eu-west-1",
            timeformat="auto",
            dateformat="auto"
        )
        
        lines = result.split('\n')
        
        # Check format handling
        assert "FORMAT AS" in lines
        assert "PARQUET" in lines
        
        # Check delimiter, quote, region, timeformat, dateformat handling
        assert "DELIMITER '\t'" in result
        assert "QUOTE \"'\"" in result
        assert "REGION AS 'eu-west-1'" in result
        assert "TIMEFORMAT 'auto'" in result
        assert "DATEFORMAT as 'auto'" in result

    def test_build_copy_statement_empty_string_overrides(self):
        """Test that empty strings can override config values for all parameters."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            s3_bucket_name="",  # Should override config default
            s3_directory="",    # Should override config default
            aws_iam_role="",    # Should override config default
            aws_region="",      # Should override config default
            redshift_cluster="" # Should override config default
        )
        
        # With empty bucket, should result in s3:///data.csv (which is invalid but shows override worked)
        assert "FROM 's3:///data.csv'" in result
        # With empty IAM role, should not include IAM_ROLE line
        assert "IAM_ROLE" not in result
        # With empty region, should not include REGION line in defaults
        assert "REGION AS" not in result

    def test_build_copy_statement_empty_database_fallback(self):
        """Test that empty database name falls back to default."""
        result = self.redconn.build_copy_statement(
            redshift_schema_name="public",
            redshift_table_name="test_table",
            source_file="data.csv",
            redshift_database=""  # Should fall back to 'default-db'
        )
        
        # Should use default-db instead of empty string
        assert "COPY default-db.public.test_table" in result
