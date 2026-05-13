import os
import sqlalchemy
from dotenv import load_dotenv, find_dotenv

# Load credentials robustly regardless of where the script is run from
load_dotenv(find_dotenv())
DB_USER = os.getenv('TIDES_DB_USER')
DB_PASS = os.getenv('TIDES_DB_PASS')
DB_HOST = os.getenv('TIDES_DB_HOST')
DB_PORT = os.getenv('TIDES_DB_PORT')
DB_NAME = os.getenv('TIDES_DB_DATABASE')

if None in [DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME]:
    print("ERROR: Missing database credentials in .env file.")
    exit(1)

# Connect to database
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(connection_string)

print(f"!!! WARNING: ABOUT TO WIPE ALL DATA IN {DB_NAME} ON {DB_HOST}:{DB_PORT} !!!")
confirm = input("Type 'RESET' to confirm: ")

if confirm != 'RESET':
    print("Aborted.")
    exit(1)

print("Executing wipe...")

with engine.connect() as conn:
    # 1. Truncate table and reset sequence
    conn.execute(sqlalchemy.text("TRUNCATE tides_master RESTART IDENTITY CASCADE;"))
    conn.execute(sqlalchemy.text("ALTER SEQUENCE tides_seq RESTART WITH 0;"))
    
    # 2. Re-apply the fixed trigger definition
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file = os.path.join(script_dir, 'createMasterTable.sql')
    with open(sql_file, 'r') as f:
        sql_commands = f.read()
        # SQLAlchemy requires text() wrapper for raw SQL
        conn.execute(sqlalchemy.text(sql_commands))
        
    conn.commit()

print(f"Hard reset complete. Naming sequence for {DB_NAME} is back at 'aaa'.")
