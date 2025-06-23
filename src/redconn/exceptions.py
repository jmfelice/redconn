"""Common exceptions for Redshift connector modules."""

class AuthenticationError(Exception):
    """Exception raised for authentication related errors."""
    pass

class ConnectionError(Exception):
    """Exception raised for connection related errors."""
    pass

class QueryError(Exception):
    """Exception raised for query execution errors."""
    pass 