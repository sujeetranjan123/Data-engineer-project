# configuration for medallion architecture
from dataclasses import dataclass

@dataclass
class S3Paths:
    bronze: str
    silver: str
    gold: str

@dataclass
class MySQLConfig:
    host: str
    port: int
    database: str
    table: str
    user: str
    jdbc_url: str

# example instantiation (values will be supplied in main)
