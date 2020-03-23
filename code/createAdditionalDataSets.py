import requests
import pandas as pd
import pycountry
from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import datetime
import os

def iso3ToIso2(row):
    country = pycountry.countries.get(alpha_3=row['iso3'])
    if country:
        return country.alpha_2
    else:
        return None

def getOECDCorporateTaxRate():
    url = 'https://stats.oecd.org/SDMX-JSON/data/CTS_CIT/.COMB_CIT_RATE/all'
    params = [('startTime','2018'),('endTime','2018')]
    r = requests.get(url, params=params)
    content = r.json()
    countries = content['structure']['dimensions']['series'][0]['values']
    values = content['dataSets'][0]['series']

    #Create Data Frame
    df = pd.DataFrame(columns=['name','iso3','corporateTaxRate'])
    for i in range(0,len(countries)):
        row = {
            'name':countries[i]['name'],
            'iso3':countries[i]['id'],
            'corporateTaxRate':values[str(i)+':0']['observations']['0'][0]
        }
        
        df = df.append(row, ignore_index=True)

    #Add iso2 based on iso3
    df['iso2'] = df.apply(lambda row: iso3ToIso2(row),axis=1)

    #Remove entries that have no iso2 code
    df = df[~df['iso2'].isna()]

    #Remove iso3
    df.drop('iso3', axis=1, inplace=True)

    return df

def getWorldBankPopGdp(attribute):
    if attribute == 'pop':
        url = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
    elif attribute == 'gdp':
        url = "http://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD"
    else:
        return None
    
    params = [('mrnev','1'),('format','json'),('per_page','300')]
    r = requests.get(url, params=params)

    #Create Data Frame
    df = pd.DataFrame(columns=['name','iso3','date',attribute])
    for record in r.json()[1]:
        row = {
            'name':record['country']['value'],
            'iso3':record['countryiso3code'],
            'date':record['date'],
            attribute:record['value']
        }
        
        df = df.append(row, ignore_index=True)

    #Remove all countries that have no iso3 code
    df = df[df['iso3'] != '']

    #Remove all countries that have date <= 2010
    df = df[df['date'] > '2010']

    #Add iso2 based on iso3
    df['iso2'] = df.apply(lambda row: iso3ToIso2(row),axis=1)

    #Remove entries that have no iso2 code
    df = df[~df['iso2'].isna()]

    #Remove iso3
    df.drop('iso3', axis=1, inplace=True)

    #Set attribute to float64
    df[attribute] = df[attribute].astype('float64')

    return df

def queryWikidata(query):

    def extractJsonResults(results):
        cols = results['head']['vars']
        df = pd.DataFrame(columns=cols)
        dict_list = []
        for row in results['results']['bindings']:
            df_dict = {}
            for col in cols:
                try:
                    value = row[col]['value']
                except:
                    value = None
                
                df_dict[col] = value
            
            dict_list.append(df_dict)
    
        df = df.append(dict_list,ignore_index=True)
        return df

    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    #Set different user aggent to fix 403 errors
    sparql.addCustomHttpHeader('User-Agent',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36')

    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    results = sparql.query().convert()

    df = extractJsonResults(results)
    return df

def getWikidataPopGdp(attribute):
    query = """
    SELECT ?iso2 ?{value} ?date WHERE {{
      ?country wdt:P297 ?iso2 .
      ?country p:{predicate} ?{value}_statement .
      ?{value}_statement ps:{predicate} ?{value} .
      OPTIONAL {{?{value}_statement pq:P585 ?date}}
    
      FILTER NOT EXISTS {{
        ?country p:{predicate} ?{value}_statement2 .
        ?{value}_statement2 pq:P585 ?date2 .
        FILTER (?date2 > ?date)
      }}
    }}
    """

    if attribute == 'pop':
        query = query.format(value='pop', predicate='P1082')
    elif attribute == 'gdp':
        query = query.format(value='gdp', predicate='P2131')
    else:
        return
    
    df=queryWikidata(query)

    #Transfrom to datetime
    df['date'] = pd.to_datetime(df['date'])

    #remove duplicated iso2 by keeping only the most recent ones
    #sometimes there are multiple entities with the same iso2 code
    #e.g. Denmark and Kingdom of Denmark
    #in that case we use the most recent value of the two entities
    #sometimes there are multiple values with the same date for an iso2 code
    #therefore, we sort by date AND attribute, so that always the largest value
    #is taken, which makes it more deterministic
    df = df.sort_values(['date',attribute], ascending=False).drop_duplicates('iso2')

    #Set attribute to float64
    df[attribute] = df[attribute].astype('float64')

    return df

def getWikidataCountryEntities():
    query = """
    SELECT ?countryEntity ?iso2 WHERE {{
    ?countryEntity wdt:P297 ?iso2 .
    }}
    """

    return queryWikidata(query)

def getWikidataCompanyEntities():
    query = """
    SELECT ?companyEntity ?LEI WHERE {{
    ?companyEntity wdt:P1278 ?LEI .
    }}
    """

    return queryWikidata(query)

def getPyCountryNames():
    return pd.DataFrame([{'iso2': c.alpha_2, 'name': c.name} for c in list(pycountry.countries)])

df_pop_world_bank = getWorldBankPopGdp('pop')
df_gdp_world_bank = getWorldBankPopGdp('gdp')

df_pop_wiki = getWikidataPopGdp('pop')
df_gdp_wiki = getWikidataPopGdp('gdp')

#Check which countries are in wiki data but not in world bank data
df_pop_wiki.loc[~df_pop_wiki['iso2'].isin(df_pop_world_bank['iso2']), 'iso2']
len(df_pop_wiki.loc[~df_pop_wiki['iso2'].isin(df_pop_world_bank['iso2']), 'iso2'])

df_gdp_wiki.loc[~df_gdp_wiki['iso2'].isin(df_gdp_world_bank['iso2']), 'iso2']
len(df_gdp_wiki.loc[~df_gdp_wiki['iso2'].isin(df_gdp_world_bank['iso2']), 'iso2'])

#Add every country that is in wiki data but not in world bank data to world bank data
df_pop = df_pop_world_bank[['iso2','pop']].copy(deep=True)
df_pop = df_pop.append(df_pop_wiki.loc[~df_pop_wiki['iso2'].isin(df_pop['iso2']), ['iso2','pop']])

df_gdp = df_gdp_world_bank[['iso2','gdp']].copy(deep=True)
df_gdp = df_gdp.append(df_gdp_wiki.loc[~df_gdp_wiki['iso2'].isin(df_gdp['iso2']), ['iso2','gdp']])

df_corporateTaxRate = getOECDCorporateTaxRate()
df_countryEntities = getWikidataCountryEntities()
df_companyEntities = getWikidataCompanyEntities()
df_countryNames = getPyCountryNames()

#Save dataframes to process them for the building of the knowledge graph
date_and_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
path_df = os.path.realpath(
    '../data/additionalData/' + date_and_time + '_df.pkl')

df_dict = {
    'df_pop_world_bank':df_pop_world_bank,
    'df_gdp_world_bank':df_gdp_world_bank,
    'df_pop_wiki':df_pop_wiki,
    'df_gdp_wiki':df_gdp_wiki,
    'df_pop':df_pop,
    'df_gdp':df_gdp,
    'df_corporateTaxRate':df_corporateTaxRate,
    'df_countryEntities':df_countryEntities,
    'df_companyEntities':df_companyEntities,
    'df_countryNames':df_countryNames
}

with open(path_df, 'wb') as output:
    pickle.dump(df_dict, output, pickle.HIGHEST_PROTOCOL)

