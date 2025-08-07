from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from aws.aws_secrets import get_secret
from dotenv import load_dotenv


def get_connection():

    load_dotenv()

    secrets = get_secret()

    url_object = URL.create(
        drivername="postgresql+psycopg2",
        database=secrets["dbname"],
        username=secrets["username"],
        password=secrets["password"],
        host=secrets["host"],
        port=secrets["port"]
    )
    try:
        engine = create_engine(url_object)
        print(f"Database connection successful")
    except Exception as e:
        print(f"Database connection failed:,{e}")

    return engine

if __name__ == "__main__":
    engine = get_connection()
