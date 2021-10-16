import pandas
import sqlalchemy as db
from decimal import Decimal


# Calculates the most bought product and then applies it's name to Most_bought
def calculate_most_bought(dataframerow):
    temporary_variable = 0
    # This for loop goes through the list of possible product expenses and finds the most bought one
    # Then adds a new column with that product's name
    for item in ['Wines', 'Fruits', 'MeatProducts', 'FishProducts', 'SweetProducts', 'GoldProds']:
        if dataframerow['Mnt{0}'.format(item)] > temporary_variable:
            temporary_variable = dataframerow['Mnt{0}'.format(item)]
            dataframerow['Most_Bought'] = item
    return dataframerow


# Replaces all default values of Education_key and Marital_Status_key with values dependent on the Education
# or Marital_Status columns in dataframe row
def add_foreign_keys(dataframerow):
    # This loop goes through the list of possible educations and then applies that education's id to use as a foreign key
    for education in [['PhD', 0], ['Graduation', 1], ['Master', 2], ['2n Cycle', 3], ['Basic', 4]]:
        if dataframerow['Education'] == education[0]:
            dataframerow['Education_key'] = education[1]
    # This loop goes through the list of possible marital statuses and then applies that marital statuses' id to use as a foreign key
    for marital_status in [['Single', 0], ['Married', 1], ['Together', 2], ['Divorced', 3]]:
        if dataframerow['Marital_Status'] == marital_status[0]:
            dataframerow['Marital_Status_key'] = marital_status[1]
    return dataframerow


# Converts all amounts in dataframe row to Decimal()
def convert_to_decimal(dataframerow):
    for item in ['Income', 'MntWines', 'MntFruits', 'MntMeatProducts', 'MntFishProducts', 'MntSweetProducts', 'MntGoldProds']:
        dataframerow[item] = Decimal(dataframerow[item])
    return dataframerow


class Scripts:
    # Creates an engine, connects to the database and grabs metadata
    def __init__(self, connectioninfo):
        self.engine = db.create_engine(connectioninfo)
        self.metadata = db.MetaData()
        self.connection = self.engine.connect()

    # Reads the csv file using pandas' read_csv(). then converts 1/0 in it to boolean True/False, after which
    # uses apply() to convert all price integers to Decimal()
    def read_csv_file(self, csvfilename):
        self.filedata = pandas.read_csv(csvfilename, sep=r'\t', engine='python')
        for number in range(1, 6):
            self.filedata['AcceptedCmp{0}'.format(number)] = self.filedata['AcceptedCmp{0}'.format(number)].astype('bool')
        self.filedata['Complain'] = self.filedata['Complain'].astype('bool')
        self.filedata['Response'] = self.filedata['Response'].astype('bool')
        self.filedata = self.filedata.apply(convert_to_decimal, axis=1)

    # This creates two extra dataframes to later send to sql using the pandas' to_sql() function
    def create_extra_dataframes(self):
        self.education_dataframe = pandas.DataFrame(data={'Name': ['PhD', 'Graduation', 'Master', '2n Cycle', 'Basic']})
        self.marital_status_dataframe = pandas.DataFrame(data={'Name': ['Single', 'Married', 'Together', 'Divorced']})

    # This uses the pandas apply() function to iterate over the dataframe and add foreign key as well as
    # find the most bought product for each customer
    def apply_most_expensive_and_keys(self):
        self.filedata['Most_Bought'] = 'string'
        self.filedata['Marital_Status_key'] = 0
        self.filedata['Education_key'] = 0
        self.filedata = self.filedata.apply(calculate_most_bought, axis=1)
        self.filedata = self.filedata.apply(add_foreign_keys, axis=1)
        self.filedata.drop(['Education'], axis=1)
        self.filedata.drop(['Marital_Status'], axis=1)

    # This uses pandas' to_sql() function to create and send three tables, or replace them if they exist
    def send_data_to_sql(self):
        self.filedata.to_sql('list', con=self.engine, if_exists='replace')
        self.education_dataframe.to_sql('educations', con=self.engine, if_exists='replace')
        self.marital_status_dataframe.to_sql('maritalstatuses', con=self.engine, if_exists='replace')

    # This uses sqlalchemy to query the database for the chosen filter type, pandas' read_sql_query()
    # in order to instantly recieve a dataframe that is then written to a xlsx file using to_excel()
    def query_sql(self, filterqueryid=0):
        table = db.Table('list', self.metadata, autoload=True, autoload_with=self.engine)   # Load table difinition from db
        query = db.select([table])  # Setup default query in case filterqueryid is a weird number
        if filterqueryid == 1:
            query = db.select([table]).where(table.columns.MntWines >= 1000)
            pandas.read_sql_query(query, self.connection).to_excel('BigWinesBuyers.xlsx')
        elif filterqueryid == 2:
            query = db.select([table]).where(table.columns.NumWebPurchases > table.columns.NumStorePurchases)
            pandas.read_sql_query(query, self.connection).to_excel('WebBuyers.xlsx')
        elif filterqueryid == 3:
            query = db.select([table]).where(table.columns.NumStorePurchases > table.columns.NumWebPurchases)
            pandas.read_sql_query(query, self.connection).to_excel('StoreBuyers.xlsx')
        elif filterqueryid == 4:
            query = db.select([table]).where(table.columns.Education == 'PhD')
            pandas.read_sql_query(query, self.connection).to_excel('CustomersWithPhD.xlsx')
        elif filterqueryid == 5:
            query = db.select([table]).where(table.columns.Marital_Status == 'Single')
            pandas.read_sql_query(query, self.connection).to_excel('SinglePartnersAroundYou.xlsx')
        else:
            pandas.read_sql_query(query, self.connection).to_excel('defaultoutput.xlsx')
