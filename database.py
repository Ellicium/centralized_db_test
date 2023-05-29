from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib


sqlServerName="gtosqlserverdev.database.windows.net"
sqlDatabaseName="gtosqldbdev"
sqlUserName="gtoadmin"
sqlPassword="Admin@gt0"
sqlSchemaName="central_data_test"
driver = "{ODBC Driver 17 for SQL Server}"
params = urllib.parse.quote_plus(f"""Driver={driver};
                                Server=tcp:{sqlServerName},1433;
                                Database={sqlDatabaseName};
                                Uid={sqlUserName};Pwd={sqlPassword};
                                Encrypt=yes;
                                TrustServerCertificate=no;
                                Connection Timeout=30;""")
SQLALCHEMY_DATABASE_URL= 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
# SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
# SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"

engine = create_engine(SQLALCHEMY_DATABASE_URL,fast_executemany=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()