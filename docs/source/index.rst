.. redconn documentation master file, created by
   sphinx-quickstart on Mon Jun 23 11:31:21 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

redconn documentation
=====================

A simple Python connector for Amazon Redshift and similar databases.

Project Home: https://github.com/jmfelice/redconn.git

Quick Start
-----------

.. code-block:: python

    from redconn import RedConn
    conn = RedConn()
    conn.connect()
    df = conn.fetch('SELECT 1')
    print(df)

Simple Usage
------------

.. code-block:: python

    # Using a context manager
    from redconn import RedConn
    with RedConn() as conn:
        df = conn.fetch('SELECT * FROM my_table LIMIT 5')
        print(df)

API Reference
-------------

.. automodule:: redconn.connector
    :members:
    :undoc-members:
    :show-inheritance:

.. toctree::
   :maxdepth: 2
   :caption: Contents:

