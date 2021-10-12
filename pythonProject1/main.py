from scripts import readcsv, getengine, tosql, filtersql, getmetad, connectdb, getinfo
import os


inp = readcsv('marketing_campaign.csv')  # Read file
engine = getengine(getinfo())  # Get engine info
tosql(engine, inp)  # Write input to db
metadata = getmetad()  # Get metadata
connection = connectdb(engine)  # Get connection info
filterinp = 4  # 0 = no filter; 1 = wine => 1000; 2 = web > shop; 3 = shop > web; 4 = phd is true; 5 = singles
filtersql(metadata, engine, connection, filterinp)
