"""
Database Connectors

Universal database connectivity for loading data into any database.
"""

from .azure_sql import AzureSQLConnector
from .postgresql import PostgreSQLConnector
from .mysql import MySQLConnector
from .mongodb import MongoDBConnector
from .snowflake import SnowflakeConnector
from .bigquery import BigQueryConnector
from .redshift import RedshiftConnector
from .dynamodb import DynamoDBConnector
from .supabase import SupabaseConnector
from .planetscale import PlanetScaleConnector
from .cockroachdb import CockroachDBConnector
from .elasticsearch import ElasticsearchConnector
from .redis import RedisConnector
from .firebase import FirebaseConnector
from .sqlite import SQLiteConnector
from .oracle import OracleConnector
from .sqlserver import SQLServerConnector
from .mariadb import MariaDBConnector
from .cassandra import CassandraConnector
from .clickhouse import ClickHouseConnector

__all__ = [
    "AzureSQLConnector",
    "PostgreSQLConnector",
    "MySQLConnector",
    "MongoDBConnector",
    "SnowflakeConnector",
    "BigQueryConnector",
    "RedshiftConnector",
    "DynamoDBConnector",
    "SupabaseConnector",
    "PlanetScaleConnector",
    "CockroachDBConnector",
    "ElasticsearchConnector",
    "RedisConnector",
    "FirebaseConnector",
    "SQLiteConnector",
    "OracleConnector",
    "SQLServerConnector",
    "MariaDBConnector",
    "CassandraConnector",
    "ClickHouseConnector",
]
