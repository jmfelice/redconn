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
            database="test-db"
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
    
    def test_build_copy_statement_with_iam_role(self):
        """Test building COPY statement with IAM role."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/MyRole",
            aws_region="us-east-1"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv"
        )
        
        expected_parts = [
            "COPY my_table FROM 's3://my-bucket/data.csv'",
            "iam_role 'arn:aws:iam::123456789012:role/MyRole'",
            "region 'us-east-1'",
            "format as csv"
        ]
        
        for part in expected_parts:
            assert part in statement
        assert statement.endswith(";")
    
    def test_build_copy_statement_with_access_keys(self):
        """Test building COPY statement with access keys."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_session_token="token123",
            aws_region="us-west-2"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv"
        )
        
        expected_parts = [
            "access_key_id 'AKIAIOSFODNN7EXAMPLE'",
            "secret_access_key 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'",
            "session_token 'token123'",
            "region 'us-west-2'"
        ]
        
        for part in expected_parts:
            assert part in statement
    
    def test_build_copy_statement_json_format(self):
        """Test building COPY statement for JSON format."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/MyRole"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.json",
            format="JSON"
        )
        
        assert "format as json 'auto'" in statement
        assert "delimiter" not in statement
        assert "ignoreheader" not in statement
    
    def test_build_copy_statement_with_gzip_and_manifest(self):
        """Test building COPY statement with gzip and manifest options."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/MyRole"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/manifest.json",
            gzip=True,
            manifest=True
        )
        
        assert "gzip" in statement
        assert "manifest" in statement
    
    def test_build_copy_statement_with_additional_options(self):
        """Test building COPY statement with additional options."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/MyRole"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv",
            additional_options={
                "ESCAPE": True,
                "NULL AS": "\\N",
                "ACCEPTINVCHARS": "?"
            }
        )
        
        assert "ESCAPE" in statement
        assert "NULL AS '\\N'" in statement
        assert "ACCEPTINVCHARS '?'" in statement
    
    def test_build_copy_statement_no_credentials_error(self):
        """Test building COPY statement raises error when no credentials provided."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db"
        )
        
        redconn = MockableRedConn(config=config)
        
        with pytest.raises(ValueError, match="Either iam_role or access_key_id/secret_access_key must be provided"):
            redconn.build_copy_statement(
                table="my_table",
                s3_path="s3://my-bucket/data.csv"
            )
    
    def test_build_copy_statement_custom_formats(self):
        """Test building COPY statement with custom date/time formats."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/MyRole"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv",
            timeformat="YYYY-MM-DD HH:MI:SS",
            dateformat="MM/DD/YYYY",
            delimiter="|",
            ignoreheader=0
        )
        
        assert "TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'" in statement
        assert "DATEFORMAT 'MM/DD/YYYY'" in statement
        assert "delimiter '|'" in statement
        assert "ignoreheader 0" in statement
    
    def test_build_copy_statement_escapes_quotes(self):
        """Test that COPY statement properly escapes single quotes."""
        config = RedshiftConfig(
            host="test-host",
            username="test-user",
            password="test-pass",
            database="test-db",
            iam_role="arn:aws:iam::123456789012:role/My'Role"
        )
        
        redconn = MockableRedConn(config=config)
        
        statement = redconn.build_copy_statement(
            table="my_table",
            s3_path="s3://my-bucket/data.csv"
        )
        
        assert "iam_role 'arn:aws:iam::123456789012:role/My''Role'" in statement 
