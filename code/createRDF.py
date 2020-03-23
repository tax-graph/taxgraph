import pandas as pd
import rdflib
import pickle
import datetime

import helpFunctions

#specify paths
path_lei_data = '../data/gleifData/20191009-0800-gleif-goldencopy-lei2-golden-copy.csv'
path_wikidata_cities = '../data/wikidataCityData/wikidata_cities.csv'
path_additonal_data = '../data/additionalData/2020-03-17_00:56:06_df.pkl'
path_relationship_data = '../data/gleifData/20191009-0800-gleif-goldencopy-rr-golden-copy.csv'
graph_storage_folder = '../data/graphData/'

#load lei data
lei_data = helpFunctions.loadLEIData(path_lei_data)
print('lei data loaded')

#load wikidata city entities
wikidataCityDict = helpFunctions.createWikidataCityDict(path_wikidata_cities)
print('wikidataCityDict created')

#add wikidata cityID and wikidata cityID_label to lei_data
(legal_cityID_list, legal_cityID_label_list, 
headquarters_cityID_list, headquarters_cityID_label_list) = helpFunctions.createMatchingCityID(lei_data, wikidataCityDict, 0.3)
lei_data['Entity_LegalAddress_CityID'] = legal_cityID_list
lei_data['Entity_LegalAddress_CityID_Label'] = legal_cityID_label_list
lei_data['Entity_HeadquartersAddress_CityID'] = headquarters_cityID_list
lei_data['Entity_HeadquartersAddress_CityID_Label'] = headquarters_cityID_label_list
print('cityID and cityID_label added to lei data')

#load additonal data
with open(path_additonal_data, 'rb') as output:
    additional_data = pickle.load(output)

#add wikidata company entity to lei_data
df_company_entities = additional_data['df_companyEntities']
lei_data = lei_data.merge(df_company_entities, how='left', on='LEI')

#create graph g
g = rdflib.Graph(identifier='taxGraph')
ns = 'http://taxgraph.informatik.uni-mannheim.de/resource/'
ns_predicate = ns + 'predicate/'

#define predicates for lei_data
predicatesLEI = dict({
    'legalName':{'colName':'Entity_LegalName', 'asLiteral': True},
    'legalAddressMailRouting':{'colName':'Entity_LegalAddress_MailRouting', 'asLiteral': True},
    'legalAddressAddressLine0':{'colName':'Entity_LegalAddress_FirstAddressLine', 'asLiteral': True},
    'legalAddressAddressLine1':{'colName':'Entity_LegalAddress_AdditionalAddressLine_1', 'asLiteral': True},
    'legalAddressAddressLine2':{'colName':'Entity_LegalAddress_AdditionalAddressLine_2', 'asLiteral': True},
    'legalAddressAddressLine3':{'colName':'Entity_LegalAddress_AdditionalAddressLine_3', 'asLiteral': True},
    'legalAddressCity':{'colName':'Entity_LegalAddress_City', 'asLiteral': True},
    'legalAddressRegion':{'colName':'Entity_LegalAddress_Region', 'asLiteral': False, 'prefix':'region/'},
    'legalAddressCountry':{'colName':'Entity_LegalAddress_Country', 'asLiteral': False, 'prefix':'country/'},
    'legalAddressPostalCode':{'colName':'Entity_LegalAddress_PostalCode', 'asLiteral': True},
    'headquartersAddressMailRouting':{'colName':'Entity_HeadquartersAddress_MailRouting', 'asLiteral': True},
    'headquartersAddressAddressLine0':{'colName':'Entity_HeadquartersAddress_FirstAddressLine', 'asLiteral': True},
    'headquartersAddressAddressLine1':{'colName':'Entity_HeadquartersAddress_AdditionalAddressLine_1', 'asLiteral': True},
    'headquartersAddressAddressLine2':{'colName':'Entity_HeadquartersAddress_AdditionalAddressLine_2', 'asLiteral': True},
    'headquartersAddressAddressLine3':{'colName':'Entity_HeadquartersAddress_AdditionalAddressLine_3', 'asLiteral': True},
    'headquartersAddressCity':{'colName':'Entity_HeadquartersAddress_City', 'asLiteral': True},
    'headquartersAddressRegion':{'colName':'Entity_HeadquartersAddress_Region', 'asLiteral': False, 'prefix':'region/'},
    'headquartersAddressCountry':{'colName':'Entity_HeadquartersAddress_Country', 'asLiteral': False, 'prefix':'country/'},
    'headquartersAddressPostalCode':{'colName':'Entity_HeadquartersAddress_PostalCode', 'asLiteral': True},
    'registrationAuthorityID':{'colName':'Entity_RegistrationAuthority_RegistrationAuthorityID', 'asLiteral': False, 'prefix':'registrationAuthorityID/'},
    'legalForm':{'colName':'Entity_LegalForm_EntityLegalFormCode', 'asLiteral': False, 'prefix':'legalForm/'},
    'managingLOU':{'colName':'Registration_ManagingLOU', 'asLiteral': False, 'prefix':'LEI/'}, #Notice that the managing LOU is identified by its LEI
    'legalAddressCityID':{'colName':'Entity_LegalAddress_CityID', 'asLiteral': False, 'prefix':'cityID/'},
    'headquartersAddressCityID':{'colName':'Entity_HeadquartersAddress_CityID', 'asLiteral': False, 'prefix':'cityID/'}
})

#check if all colNames of the predicates can be found in lei_data
for key in predicatesLEI.keys():
    if(not predicatesLEI[key]['colName'] in lei_data.columns):
        raise ValueError(predicatesLEI[key]['colName'] + ' is not a valid column name')

#check if all predicates that do not link to a literal have a prefix defined
for key in predicatesLEI.keys():
    if(not predicatesLEI[key]['asLiteral'] and not ('prefix' in predicatesLEI[key].keys())):
        raise ValueError(key + ' is missing a prefix key')

#create the URIRef for each predicate
for key in predicatesLEI.keys():
    predicate_dict = predicatesLEI[key]
    predicate_dict['URIRef'] = rdflib.URIRef(ns_predicate + key)

##### add lei_data triples ##### 
i = 0
for t in lei_data.itertuples():
    #if LEI is nan we cant add any information
    if pd.isnull(t.LEI):
        continue
    #create a node for this LEI
    LEI = rdflib.URIRef(ns + 'LEI/' + getattr(t, 'LEI'))

    #add for each predicate a triple, if it exists
    for key in predicatesLEI.keys():
        value = getattr(t, predicatesLEI[key]['colName'])
        if not pd.isnull(value):
            if(predicatesLEI[key]['asLiteral']):
                o = rdflib.Literal(value)
            else:
                prefix = predicatesLEI[key]['prefix']
                o = rdflib.URIRef(ns + prefix + value )

            g.add( (LEI,predicatesLEI[key]['URIRef'],o) )
    
    #add sameAs predicate for LEI node
    value = getattr(t, 'companyEntity')
    if not pd.isnull(value):
        p = rdflib.URIRef('http://www.w3.org/2002/07/owl#sameAs')
        o = rdflib.URIRef(value)
        g.add( (LEI, p, o) )
    
    i+=1
    if(i%250000 == 0):
        print(i)

##### add locatedIn to region #####
#find all unique regions
all_regions = lei_data['Entity_LegalAddress_Region'].append(
    lei_data['Entity_HeadquartersAddress_Region'])
unique_regions = all_regions[~all_regions.duplicated()]

for region in unique_regions:
    if not pd.isnull(region):
        s = rdflib.URIRef(ns + 'region/' + region)
        p = rdflib.URIRef(ns_predicate + 'locatedIn')
        #the first two letters of the region correspond to the country of the region
        o = rdflib.URIRef(ns + 'country/' + region[0:2])

        g.add( (s, p, o) )

##### add sameAs to cityID #####
#find all unique cityIDs
all_cityID = lei_data['Entity_LegalAddress_CityID'].append(
    lei_data['Entity_HeadquartersAddress_CityID'])
unique_cityID = all_cityID[~all_cityID.duplicated()]

for cityID in unique_cityID:
    if not pd.isnull(cityID):
        s = rdflib.URIRef(ns + 'cityID/' + cityID)
        p = rdflib.URIRef('http://www.w3.org/2002/07/owl#sameAs')
        o = rdflib.URIRef('http://www.wikidata.org/wiki/Q' + cityID)

        g.add( (s, p, o) )

##### add label to cityID #####
#find all unique combinations of cityID and cityID label
legal_cityIDANDlabel = lei_data[['Entity_LegalAddress_CityID', 'Entity_LegalAddress_CityID_Label']]
legal_cityIDANDlabel.columns = ['cityID','label'] 

headquarters_cityIDANDlabel = lei_data[['Entity_HeadquartersAddress_CityID', 'Entity_HeadquartersAddress_CityID_Label']]
headquarters_cityIDANDlabel.columns = ['cityID','label'] 

all_cityIDANDlabel = legal_cityIDANDlabel.append(headquarters_cityIDANDlabel)
unique_cityIDANDlabel = all_cityIDANDlabel[~all_cityIDANDlabel.duplicated()]

for row in unique_cityIDANDlabel.itertuples():
    cityID = getattr(row, 'cityID')
    label = getattr(row, 'label')

    if ((not pd.isnull(cityID)) and (not pd.isnull(label))):
        s = rdflib.URIRef(ns + 'cityID/' + cityID)
        p = rdflib.URIRef('http://www.w3.org/2000/01/rdf-schema#label')
        o = rdflib.Literal(label)
        
        g.add( (s, p, o) )

##### add locatedIn to cityID #####
legal_cityIDANDregion = lei_data[['Entity_LegalAddress_CityID', 'Entity_LegalAddress_Region']]
legal_cityIDANDregion.columns = ['cityID','region'] 

headquarters_cityIDANDregion = lei_data[['Entity_HeadquartersAddress_CityID', 'Entity_HeadquartersAddress_Region']]
headquarters_cityIDANDregion.columns = ['cityID','region'] 

all_cityIDANDregion = legal_cityIDANDregion.append(headquarters_cityIDANDregion)

#group by cityID and region
#and get for each combination the number of LEI entries with that combination
all_cityIDANDregion_grp = all_cityIDANDregion.groupby(['cityID','region']).size()

#group by cityID and find the region that has the most LEI entries for that cityID
#this makes sure that each city is only located in a single region
#this results in a Series where each entry is a tuple of the from (cityID, region)
all_cityIDANDregion_max = all_cityIDANDregion_grp.groupby(level='cityID').idxmax()

for row in all_cityIDANDregion_max:
    cityID = row[0]
    region = row[1]

    if ((not pd.isnull(cityID)) and (not pd.isnull(region))):
        s = rdflib.URIRef(ns + 'cityID/' + cityID)
        p = rdflib.URIRef(ns_predicate + 'locatedIn')
        o = rdflib.URIRef(ns + 'region/' + region)
        
        g.add( (s, p, o) )

##### add country specific data #####
add_country_data_dict = {
    'df_pop':{'value_name':'pop', 'predicate_URI':ns_predicate + 'population'},
    'df_gdp':{'value_name':'gdp', 'predicate_URI':ns_predicate + 'GDP'},
    'df_corporateTaxRate':{'value_name':'corporateTaxRate', 'predicate_URI':ns_predicate + 'corporateTaxRate'},
    'df_countryNames':{'value_name':'name', 'predicate_URI':'http://www.w3.org/2000/01/rdf-schema#label'},
    'df_countryEntities':{'value_name':'countryEntity', 'predicate_URI':'http://www.w3.org/2002/07/owl#sameAs'}
}

for key, value_dict in add_country_data_dict.items():
    data = additional_data[key]
    value_name = value_dict['value_name']
    predicate_URI = value_dict['predicate_URI']

    p = rdflib.URIRef(predicate_URI)

    for row in data.itertuples():
        country = getattr(row, 'iso2')
        value = getattr(row, value_name)

        s = rdflib.URIRef(ns + 'country/' + country)
        
        if key == 'df_countryEntities':
            o = rdflib.URIRef(value)
        else:
            o = rdflib.Literal(value)

        g.add( (s, p, o) )

#load relationship_data
relationship_data = pd.read_csv(
    path_relationship_data, sep=',', header=0, index_col=False, dtype=str
)

#replace dots with underscores in the column names
relationship_data_column_names = [s.replace('.', '_') for s in relationship_data.columns]
#rename data frame columns
relationship_data.columns = relationship_data_column_names

##### add relationship_data triples #####
for t in relationship_data.itertuples():
    #if startLEI, endLEI, or type is nan we cant add any information
    if (pd.isnull(t.Relationship_StartNode_NodeID)
    or pd.isnull(t.Relationship_EndNode_NodeID)
    or pd.isnull(t.Relationship_RelationshipType)):
        continue

    #create nodes 
    startLEI = getattr(t, 'Relationship_StartNode_NodeID')
    endLEI = getattr(t, 'Relationship_EndNode_NodeID')
    s = rdflib.URIRef(ns + 'LEI/' + startLEI) 
    o = rdflib.URIRef(ns + 'LEI/' + endLEI) 

    #add relationship
    relationshipType = getattr(t, 'Relationship_RelationshipType')

    if relationshipType == 'IS_DIRECTLY_CONSOLIDATED_BY':
        p = rdflib.URIRef(ns_predicate + 'isDirectlyConsolidatedBy')
    elif relationshipType == 'IS_ULTIMATELY_CONSOLIDATED_BY':
        p = rdflib.URIRef(ns_predicate + 'isUltimatelyConsolidatedBy')
    elif relationshipType == 'IS_INTERNATIONAL_BRANCH_OF':
        p = rdflib.URIRef(ns_predicate + 'isInternationalBranchOf')

    g.add( (s, p, o) )
print('graph created')

#save graph
date_and_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
graph_storage_path = graph_storage_folder + date_and_time + '_taxGraph.xml' 
with open(graph_storage_path, 'wb') as output:
    g.serialize(
        destination=output,format='xml'
    )

#close graph
g.close()
