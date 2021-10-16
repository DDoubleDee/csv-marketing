from scripts import Scripts
from dotenv import load_dotenv
import os


load_dotenv()
script = Scripts(os.getenv('CONNECTION_URL'))
script.read_csv_file('marketing_campaign.csv')
script.create_extra_dataframes()
script.apply_most_expensive_and_keys()
script.send_data_to_sql()
i = 5
# If == 1 creates file with customers who paid more than 1000 for wine
# If == 2 creates file with customers who buy more from the web
# If == 3 creates file with customers who buy more from the store
# If == 4 creates file with customers who have PhD
# If == 5 creates file with customers who are single
script.query_sql(5)
