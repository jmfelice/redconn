import re

def _sanitize_log_message(message: str) -> str:
    """Sanitize log messages to prevent sensitive data exposure.
    Args:
        message: The log message to sanitize
    Returns:
        str: Sanitized log message with sensitive data masked
    """
    # Mask potential password patterns
    message = re.sub(r'password\s*=\s*[^\s,)]+', 'password=*****', message, flags=re.IGNORECASE)
    message = re.sub(r'pwd\s*=\s*[^\s,)]+', 'pwd=*****', message, flags=re.IGNORECASE)
    # Mask potential connection strings
    message = re.sub(r'(jdbc|postgresql|redshift):[^;\s]+', '\\1:*****', message)
    # Mask potential AWS keys
    message = re.sub(r'(?i)(aws_access_key_id|aws_secret_access_key|aws_session_token)\s*=\s*[^\s,)]+', '\\1=*****', message)
    return message

def _sanitize_query(query: str) -> str:
    """Sanitize SQL query for logging to prevent sensitive data exposure.
    Args:
        query: The SQL query to sanitize
    Returns:
        str: Sanitized query with sensitive data masked
    """
    # Mask literals in INSERT statements
    query = re.sub(r"(INSERT\s+INTO\s+[^\s]+\s+VALUES\s*\()([^)]+)(\))", r"\1*****\3", query, flags=re.IGNORECASE)
    # Mask literals in UPDATE SET clauses
    query = re.sub(r"(SET\s+[^\s=]+\s*=\s*)'[^']*'", r"\1'*****'", query, flags=re.IGNORECASE)
    # Mask connection strings
    query = re.sub(r"'(jdbc|postgresql|redshift):[^']*'", "'\\1:*****'", query)
    return query 