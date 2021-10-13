import pandas
import sqlalchemy as db


def getinfo():
    return open('.env', 'r').read()


def readcsv(csv):
    inp = pandas.read_csv(csv, sep=r'\t', engine='python')
    for i in range(1, 6):
        inp['AcceptedCmp{0}'.format(i)] = inp['AcceptedCmp{0}'.format(i)].astype('boolean')
    inp['Complain'] = inp['Complain'].astype('boolean')
    inp['Response'] = inp['Response'].astype('boolean')
    inp['Education'] = inp['Education'].astype('category')
    inp['Marital_Status'] = inp['Marital_Status'].astype('category')
    inp['Income'] = inp['Income'].astype('float')
    mnt = ['Wines', 'Fruits', 'MeatProducts', 'FishProducts', 'SweetProducts', 'GoldProds']
    for i in mnt:
        inp['Mnt{0}'.format(i)] = inp['Mnt{0}'.format(i)].astype('float')
    return inp


def getengine(confo):
    return db.create_engine(confo)


def getmetad():
    return db.MetaData()


def connectdb(engine):
    return engine.connect()


def tosql(engine, inp):
    mnt = ['Wines', 'Fruits', 'MeatProducts', 'FishProducts', 'SweetProducts', 'GoldProds']
    inp['MostExpense'] = 'one'
    for i in range(2240):
        comp = 0
        for i2 in mnt:
            if int(comp) < inp['Mnt{0}'.format(i2)][i]:
                comp = inp['Mnt{0}'.format(i2)][i]
                inp['MostExpense'][i] = i2
    inp.to_sql('inputlist', con=engine, if_exists='replace')
    for i in ['Phd', 'Graduation', 'Master', '2n Cycle', 'Basic']:
        i2 = inp.loc[inp['Education'] == i]
        i2['foreign_key'] = i2['ID']
        i2.drop(['ID'], axis=1)
        i2.to_sql(i, con=engine, if_exists='replace')
    for i in ['Single', 'Married', 'Together', 'Divorced']:
        i2 = inp.loc[inp['Marital_Status'] == i]
        i2['foreign_key'] = i2['ID']
        i2.drop(['ID'], axis=1)
        i2.to_sql(i, con=engine, if_exists='replace')


def filtersql(metadata, engine, connection, filterinp=0):
    table = db.Table('inputlist', metadata, autoload=True, autoload_with=engine)
    q = db.select([table])
    if filterinp == 1:
        q = db.select([table]).where(table.columns.MntWines >= 1000)
    if filterinp == 2:
        q = db.select([table]).where(table.columns.NumWebPurchases > table.columns.NumStorePurchases)
    if filterinp == 3:
        q = db.select([table]).where(table.columns.NumStorePurchases > table.columns.NumWebPurchases)
    if filterinp == 4:
        q = db.select([table]).where(table.columns.Education == 'PhD')
    if filterinp == 5:
        q = db.select([table]).where(table.columns.Marital_Status == 'Single')
    pandas.read_sql_query(q, connection).to_excel('output.xlsx')
