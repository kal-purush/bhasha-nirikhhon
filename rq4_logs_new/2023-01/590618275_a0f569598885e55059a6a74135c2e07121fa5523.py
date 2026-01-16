from dotenv import load_dotenv
import os

load_dotenv()


user =os.environ["MYSQL_USER"]
host =os.environ["MYSQL_HOST"]
database=os.environ["DATABASE"]

