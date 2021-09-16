import pandas as pd
import os
import glob
import re
import json
import csv
import decimal
import mysql.connector
from mysql.connector import errorcode
import geopandas as gp
import warnings
import holoviews as hv
hv.extension('bokeh')

warnings.filterwarnings('ignore')
gp.datasets.available

path = os.getcwd()
csv_files = glob.glob(os.path.join(path, 'Data Files\*.csv'))
  
with open('Data Files\countries_continents_codes_flags_url.json') as json_file:
    json_data = json.load(json_file)


#MySQL connection
try:
    cnx = mysql.connector.connect(user='Sam', password='Saamiyah0107',
                              host='localhost',
                              database='worldhappiness')
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)


######################################################################################################################################
#Part 2: Automated Data Pipeline
######################################################################################################################################

#Insert data into databases
for f in csv_files:    
    # read the csv file
    df = pd.read_csv(f)

    #Create cursor connection
    cursor = cnx.cursor()
    
    new_names =  {
        'Country or region' : 'Country',
        'Score' : 'HappinessScore',
        'Happiness.Score' : 'HappinessScore',
        'Happiness Score' : 'HappinessScore',
        'GDP per capita' : 'Economy',
        'Economy..GDP.per.Capita.' : 'Economy',
        'Economy (GDP per Capita)' : 'Economy',
        'Social support' : 'Family',
        'Healthy life expectancy' : 'LifeExpectancy',
        'Health..Life.Expectancy.' : 'LifeExpectancy',
        'Health (Life Expectancy)' :  'LifeExpectancy',
        'Freedom to make life choices' : 'Freedom',
        'Perceptions of corruption' : 'Trust',
        'Trust..Government.Corruption.' : 'Trust',
        'Trust (Government Corruption)' : 'Trust',
        'Dystopia.Residual' : 'DystopiaResidual',
        'Dystopia Residual' : 'DystopiaResidual'
    }
    df.rename(columns=new_names, inplace=True)

    #Get year through report name
    year = re.compile(r'\d+').search(f).group(0)

    #Null values in csv
    df.drop(df.query('Trust.isnull()').index, inplace=True)

    #Loop through DataFrame
    for row in df.itertuples():
      if 'DystopiaResidual' in df.columns: dystopiaResidual = row.DystopiaResidual
      else: dystopiaResidual = None

      if 'Family' in df.columns: family = row.Family
      else: family = None

      cursor.execute('''
                INSERT IGNORE INTO Country(Name) VALUES (%s);
            ''',(row.Country,))
      cursor.execute('''
                INSERT INTO WorldHappinessDetails(CountryId, Year, HappinessScore, Family, Economy, LifeExpectancy, Freedom, GovernmentCorruption, Generosity, DystopiaResidual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''',(cursor.lastrowid, year, row.HappinessScore, family, row.Economy, row.LifeExpectancy, row.Freedom, row.Trust, row.Generosity,dystopiaResidual))

cnx.commit()



######################################################################################################################################
#Part 3: Data Modelling record in csv and parquet
######################################################################################################################################

#Data Modelling in CSV format
def write_csv_file():
  #Get data from database
  cursor.execute('''
                  SELECT Year, RANK() OVER(PARTITION BY Year ORDER BY HappinessScore DESC) AS 'Overall Rank', Name, HappinessScore, Economy, Family, LifeExpectancy, Freedom, Generosity, GovernmentCorruption
                  FROM WorldHappinessDetails LEFT JOIN Country
                  on WorldHappinessDetails.CountryId = Country.CountryId
  ''')

  #Extract row headers
  row_headers=[x[0] for x in cursor.description]

  #Fetch data from cursor
  Data = cursor.fetchall()

  json_list = []
  for result in Data:
    json_list.append(dict(zip(row_headers, result)))

  #serialize decimal 
  def dec_serializer(o):
      if isinstance(o, decimal.Decimal):
          return float(o)

  #Converts DB data to Json        
  dbJson_data = json.dumps(json_list, default=dec_serializer)
  dBJson = json.loads(dbJson_data)

  #Class for DB values
  class DbJsonClass:
      def __init__(self, Year, OverallRank, Name, HappinessScore, HappinessStatus, Economy, Family, LifeExpectancy, Freedom, Generosity, GovernmentCorruption):
        self.Year = Year
        self.OverallRank = OverallRank
        self.Name = Name
        self.HappinessScore = HappinessScore
        self.HappinessStatus = HappinessStatus
        self.Economy = Economy
        self.Family = Family
        self.LifeExpectancy = LifeExpectancy
        self.Freedom = Freedom
        self.Generosity = Generosity
        self.GovernmentCorruption = GovernmentCorruption

  #Class for Json input values
  class JsonClass:
      def __init__(self, Country, ImageUrl, RegionCode, Region):
        self.Country = Country
        self.ImageUrl = ImageUrl
        self.RegionCode = RegionCode
        self.Region = Region

  jsonList = []
  DbJsonList = []

  #Insert values from DB Json to list
  for k in dBJson:
    if (float(k.get('HappinessScore')) > 5.6) : status = "Green"
    elif (float(k.get('HappinessScore')) < 2.6 and k.get('HappinessScore') > 5.6) : status = "Amber"
    elif (float(k.get('HappinessScore') < 2.6)) : status = "Red"
    dbJsonObject = DbJsonClass(k.get('Year'), k.get('Overall Rank'), k.get('Name'), k.get('HappinessScore'), status, k.get('Economy'), k.get('Family'), k.get('LifeExpectancy'), k.get('Freedom'), k.get('Generosity'), k.get('GovernmentCorruption'))
    DbJsonList.append(dbJsonObject)


  #Insert Json values to list
  for j in json_data:
    #y = map(lambda x: 'Nan' if x in j.region else x, j.get('region').upper())
    if not j.get('region'): region = 'Nan'
    else: region = j.get('region').upper()
    jsonObject = JsonClass(j.get('country'), j.get('image_url'), j.get('region-code'), region)
    jsonList.append(jsonObject)


  #Write to CSV file
  with open('DataModelling.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Year', 'Country', 'Image url', 'Region code', 'Region', 'Overall Rank', 'Happiness Score', 'Happiness Status', 'GDP per capita', 'Family', 'Healthy life expectancy', 'Freedom to make life choices', 'Generosity', 'Perceptions of corruption'])
    for item in jsonList:
      for j in DbJsonList:
        if j.Name == item.Country:
          writer.writerow([j.Year, j.Name, item.ImageUrl, item.RegionCode, item.Region, j.OverallRank, j.HappinessScore, status, j.Economy, j.Family, j.LifeExpectancy, j.Freedom, j.Generosity, j.GovernmentCorruption])


#Data Modelling in parquet format
def write_parquet_file():
    df = pd.read_csv('DataModelling.csv')
    df.to_parquet('DataModelling.parquet')



######################################################################################################################################
#Part 4: Json format for BCM 
######################################################################################################################################


#Data Modelling in Json format
def write_json_extract():
  #Fetch data from DB
  cursor.execute('''
                  SELECT  DISTINCT(details.Name), Max(HappinessScore) AS 'Highest Happiness Score', Min(HappinessScore) AS 'Lowest Happiness Score', max(OverallRank) AS 'Highest Rank', min(OverallRank) AS 'Lowest Rank' FROM (SELECT RANK() OVER(PARTITION BY Year ORDER BY HappinessScore DESC) AS 'OverallRank', Name, HappinessScore
                  FROM WorldHappinessDetails LEFT JOIN Country
                  on WorldHappinessDetails.CountryId = Country.CountryId)details
                  GROUP BY details.Name
                  ORDER BY details.Name ASC
  ''')

  HappinessScoreData = cursor.fetchall()
  #Convert data to Json format
  json_object = json.dumps(HappinessScoreData)

  #Write to Json file
  with open("JsonExtract.json", "w") as outfile:
    outfile.write(json_object)


write_csv_file()
write_parquet_file()
write_json_extract()