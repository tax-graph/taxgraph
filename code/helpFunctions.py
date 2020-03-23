import pandas as pd
import numpy as np
import Levenshtein

def createWikidataCityDict(path):
    df = pd.read_csv(path,sep=';',dtype='str')
    
    #remove http://www.wikidata.org/entity/Q from city column
    df['city'] = df['city'].str.slice(32)

    #drop duplicated rows
    df.drop_duplicates(inplace=True)

    #drop rows with postalcode nan
    df = df[~df['postalcode'].isna()]

    def unfoldComma(df):
        #find all rows that contain a comma in postal code
        mask = df['postalcode'].str.contains(',')

        res = []
        for row in df.loc[mask,:].itertuples(index=False):
            postalcode_list = getattr(row,'postalcode').split(',')
            #remove leading and trailing whitespaces
            postalcode_list = [x.strip() for x in postalcode_list]

            city = getattr(row,'city')
            cityLabel = getattr(row,'cityLabel')

            for postalcode in postalcode_list:
                res.append({
                    'city':city,
                    'postalcode':postalcode,
                    'cityLabel':cityLabel
                })

        #remove all rows with comma in postal code
        df = df.loc[~mask,:]

        #Add all comma unfolded rows
        df = df.append(res)

        return df

    def unfoldRange(df,separator):
        #this function gets used when the current row will not be unfolded
        #since all rows of mask will be deleted, the rows that do no get unfolded
        #would get lost otherwise
        def addCurrentRow(res,row):
            res.append({
                'city':getattr(row,'city'),
                'postalcode':getattr(row,'postalcode'),
                'cityLabel':getattr(row,'cityLabel')
            })

        #find all rows that contain ONE separator in postal code
        mask = df['postalcode'].str.count(separator)
        mask = mask == 1

        res = []
        for row in df.loc[mask,:].itertuples(index=False):
            first, second = getattr(row,'postalcode').split(separator)

            city = getattr(row,'city')
            cityLabel = getattr(row,'cityLabel')

            #only process row when first and second have both no -
            first_mask = first.count('-')
            second_mask = second.count('-')
            if first_mask == 0 and second_mask == 0:
                first_len = len(first)
                second_len = len(second)

                if first_len != second_len:
                    addCurrentRow(res,row)
                    continue
                
                try:
                    first_int = int(first)
                except:
                    addCurrentRow(res,row)
                    continue

                try:
                    second_int = int(second)
                except:
                    addCurrentRow(res,row)
                    continue

                assert first_int >= 0 and second_int >= 0

                #do not unfold row when more than 100000 post codes would be added
                #if (second_int - first_int) > 10000:
                #    addCurrentRow(res,row)
                #    continue

                for i in range(first_int,second_int+1):
                    #use zfill to account for post codes that start with 0
                    #e.g. int("01184") results in 1184
                    res.append({
                        'city':city,
                        'postalcode':str(i).zfill(first_len),
                        'cityLabel':cityLabel
                    })
            else:
                addCurrentRow(res,row)
                continue
            
        #remove all rows with separator in postal code
        df = df.loc[~mask,:]

        #Add all range unfolded rows
        df = df.append(res)

        return df

    df = unfoldComma(df)
    df = unfoldRange(df, '–')

    #drop duplicates again
    df.drop_duplicates(inplace=True)

    #build dictionary from df
    grouping = df.groupby(by = 'postalcode')

    postalcode_dict = {}
    for code, data in grouping:
        postalcode_dict[code] = data
    
    return postalcode_dict

def loadLEIData(path):
    #specify which columns to load from LEI data
    cols_lei_data = [
        0,1,34,37,38,39,40,41,42,43,44,46,49,50,51,52,
        53,54,55,56,187,192,208
    ]

    #load LEI data
    lei_data = pd.read_csv(
        path, sep=',', header=0, index_col=False, usecols=cols_lei_data, dtype=str
    )
    #replace dots with underscores in the column names
    lei_data_column_names = [s.replace('.', '_') for s in lei_data.columns]
    #rename data frame columns
    lei_data.columns = lei_data_column_names

    return lei_data

def createMatchingCityID(lei_data, wikidataCityDict, max_distance):

    def matchCityID(city_name, postal_code):
        if pd.isnull(city_name) or pd.isnull(postal_code):
            return None, None

        if postal_code in wikidataCityDict:
            postal_code_data = wikidataCityDict[postal_code] 
        else:
            return None, None
        
        city_name_len = len(city_name)
        #for a given city name and postal code from the lei data
        #calculate a normalized edit distance between the city name and
        #all wikidata labels with the same postal code
        distances = [
            Levenshtein.distance(city_name.lower(), cityLabel.lower())/max(city_name_len, len(cityLabel))
            for cityLabel in postal_code_data['cityLabel'].values
        ]
        #find the wikidata label with the smallest edit distance to city name
        min_location = np.argmin(distances)

        if distances[min_location] <= max_distance:
            #return cityID and cityID_label
            return postal_code_data.iloc[min_location,0], postal_code_data.iloc[min_location,2]
        else:
            return None, None

    legal_cityID_list = []
    legal_cityID_label_list = []
    headquarters_cityID_list = []
    headquarters_cityID_label_list = []

    for row in lei_data.itertuples():
        legal_city_name = getattr(row, 'Entity_LegalAddress_City')
        legal_postal_code = getattr(row, 'Entity_LegalAddress_PostalCode')

        headquarters_city_name = getattr(row, 'Entity_HeadquartersAddress_City')
        headquarters_postal_code = getattr(row, 'Entity_HeadquartersAddress_PostalCode')

        legal_cityID, legal_cityID_label = matchCityID(legal_city_name, legal_postal_code)

        #if city name and postal code are the same for legal and headquarters address do not rerun matchCityID
        if ((legal_city_name == headquarters_city_name) and (legal_postal_code == headquarters_postal_code)):
            headquarters_cityID = legal_cityID
            headquarters_cityID_label = legal_cityID_label
        else:
            headquarters_cityID, headquarters_cityID_label = matchCityID(headquarters_city_name, headquarters_postal_code)
        
        legal_cityID_list.append(legal_cityID)
        legal_cityID_label_list.append(legal_cityID_label)
        headquarters_cityID_list.append(headquarters_cityID)
        headquarters_cityID_label_list.append(headquarters_cityID_label)

    return legal_cityID_list, legal_cityID_label_list, headquarters_cityID_list, headquarters_cityID_label_list

