import os
from dotenv import load_dotenv


load_dotenv()  # reads variables from a .env file and sets them in os.environ



def get_db_url():
    POSTGRES_SERVER = os.environ.get("POSTGRES_SERVER", "")
    
    # Check if POSTGRES_SERVER is already a full URL
    if POSTGRES_SERVER.startswith("postgresql://") or POSTGRES_SERVER.startswith("postgres://"):
        # Already a full URL, use it directly
        return POSTGRES_SERVER
    else:
        # Build the URL from components
        POSTGRES_USERNAME = os.environ["POSTGRES_USERNAME"]
        POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
        POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]
        return f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"
