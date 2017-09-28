# -*- coding: utf-8 -*-
"""
Created on Sun Sep 17 17:33:00 2017

@author: Daniel
"""

from useful_functions import remove_polish
import re
import pandas as pd
import numpy as np



def format_adress(data):
    """If adress data is not an empty string return first element (until comma)
    which us usually street or district, otherwise return None"""
    if len(data)>0:
        if ',' in data[0]:
            return data[0].split(',')[0]
        else:
            return data[0]
    else:
        return None

def format_price(data):
    """Transform price to float if it's already int or float (done when ad was
    downloaded) or return none if price is non-numeric"""
    if isinstance(data,int) or isinstance(data,float):
        return float(data)
    else:
        return None

def format_date(data):
    """Change date forma to pandas datetime object or return None"""
    try:
        return pd.to_datetime(data.replace('/','-'), dayfirst=True)
    except:
        return None
    
def format_advertiser(data):
    """Replace polish advertiser type with english or return none"""
    replace_dict = {'Właściciel':'owner', 'Agencja':'agency'}
    if data in replace_dict.keys():
        return replace_dict[data]
    else:
        return None

def format_n_rooms(data):
    """Transfrom the string with number of rooms to a float or return None"""
    try:
        # split required as the first element is a number like: "1 pokoj/room"
        # Kawalerka sth like a studio
        if data.split()[0] == 'Kawalerka':
            return 1.0
        else:
            return float(data.split()[0])
    except:
        #print("Unable to format: {}".format(data))
        return None

def format_n_bathrooms(data):
    """Transform number of bathrooms to float or return none"""
    try:
        return float(data.split()[0])
    except:
        #print("Unable to format: {}".format(data))
        return None

def format_location(data):
    """Check if the location is in one of two districts sets (with and without
    polish letters) and return non-polish version. If not in any of the sets
    return None"""
    districts = {'Praga Północ','Ursus','Wesoła','Wilanów','Praga Południe',
                 'Śródmieście','Rembertów','Ursynów','Targówek','Białołęka',
                 'Włochy','Wola','Mokotów','Bemowo','Ochota','Żoliborz',
                 'Bielany','Wawer'}
    districts_en = {'Praga Poludnie', 'Mokotow','Wesola','Srodmiescie','Bemowo',
                    'Ursus','Zoliborz','Wola','Bielany','Targowek','Wawer',
                    'Wlochy','Rembertow','Ochota','Wilanow','Bialoleka',
                    'Praga Polnoc','Ursynow'}
    if data in districts or data in districts_en:
        return remove_polish(data)
    else:
        return None

def format_desc(data):
    """Remove newlines an unnecessary spaces in the description"""
    return " ".join(data.split())

def format_smoking(data):
    """Convert smoking permission to boolean or retrun None"""
    if data=='Nie':
        return False
    elif data=='Tak':
        return True
    else:
        return None

def format_parking(data):
    """Check if parking place type is valid or return None"""
    parking = {'Garaż','Kryty','Ulica','Brak'}
    if data in parking:
        return data
    else:
        return None

def format_animals(data):
    """Convert animals permission to boolean or retrun None"""
    if data=='Nie':
        return False
    elif data=='Tak':
        return True
    else:
        return None
    
def format_type(data):
    """Check if place type is valid or return None"""
    types = {'Inne', 'Dom', 'Mieszkanie'}
    if data in types:
        return data
    else:
        return None
    
def format_size(data):
    """Convert flat/room size to float or return None"""
    try:
        return float(data)
    except:
        return None
    
def format_sharing(data):
    """Translate sharing type (shared room or not) to english or return None"""
    types = {'Współdzielenie pokoju':'room_share',
             'Współdzielenie mieszkania/domu':'flat_share'}
    if data in types.keys():
        return types[data]
    else:
        return None

def transform_dataset(dataset_dict):
    """Transforms the dataset into a dataframe with engilsh (and shorter) column
    names as well as converts multiple column contents to various data types or
    returns None if conversion fails (problem with data).
    
    Column names after conversion:
    ----------
    Flats: 
            ['adress', 'price', 'coords', 'date', 'advertiser', 'available',
            'n_rooms', 'n_bath', 'location', 'desc', 'smoking', 'parking',
            'animals', 'type', 'street_regex', 'size_m2', 'url']
    
    Rooms: 
            'adress', 'price', 'date', 'advertiser', 'available', 'n_rooms',
            'location', 'desc', 'charges', 'smoking', 'pref_gen', 'animals', 'type',
            'street_regex', 'size_m2', 'share', 'url']
    
    Data types after conversion:
    ----------
    adress : string,  
    price : float,
    date : datetime64[ns],
    advertiser : string,
    available : datetime64[ns],
    n_rooms : float,
    location : string,
    desc : string,
    charges : string,
    smoking : bool,
    pref_gen : string,
    animals : bool,
    type : string,
    street_regex : list,
    size_m2 : float,
    share : string,
    url : string,
    pref_gen : string,
    """
    # dictionary with 'original':'replacement' column name pairs
    colnames_dict = {'Adres':'adress', 'Cena':'price', 'Coordinates':'coords',
                 'Data dodania':'date', 'Do wynajęcia przez':'advertiser',
                 'Dostępny':'available', 'Liczba pokoi':'n_rooms',
                 'Liczba łazienek':'n_bath', 'Lokalizacja':'location',
                 'Opis':'desc', 'Palący':'smoking', 'Parking':'parking',
                 'Przyjazne zwierzakom':'animals', 'Rodzaj nieruchomości':'type',
                 'Ulica_re':'street_regex', 'Wielkość (m2)':'size_m2',
                 'link':'url','Opłaty':'charges','Preferowana płeć':'pref_gen',
                 'Współdzielenie':'share'}
    # transform the dict dataset to pandas DataFrame object with ads as rows
    dataset_df = pd.DataFrame(dataset_dict).transpose()
    print("Dataset transformed to a DataFrame")
    # change column names
    dataset_df.columns = list(map(lambda x:colnames_dict[x], list(dataset_df.columns)))
    print("Column names changed")
    # transform adress
    dataset_df['adress'] = dataset_df['adress'].apply(lambda x:format_adress(x))
    print("Adress format changed")
    # convert pirce
    dataset_df['price'] = dataset_df['price'].apply(lambda x:format_price(x))
    print("Prices format changed")
    # convert ad date and available date to datetime format
    dataset_df['date'] = dataset_df['date'].apply(lambda x:format_date(x))
    dataset_df['available'] = dataset_df['available'].apply(lambda x:format_date(x))
    print("Dates format changed")
    # transform advertiser type
    dataset_df['advertiser'] = dataset_df['advertiser'].apply(lambda x:format_advertiser(x))
    print("Advertiser format changed")    
    # convert number of rooms to float
    dataset_df['n_rooms'] = dataset_df['n_rooms'].apply(lambda x:format_n_rooms(x))
    print("Number of rooms format changed")
    # if number of bathrooms column is in the dataset convert it to float
    if 'n_bath' in dataset_df.columns:
        dataset_df['n_bath'] = dataset_df['n_bath'].apply(lambda x:format_n_bathrooms(x))
        print("Number of bathrooms format changed")
    # transform location format
    dataset_df['location'] = dataset_df['location'].apply(lambda x:format_location(x))
    print("Location format changed")
    # transform description format
    dataset_df['desc'] = dataset_df['desc'].apply(lambda x:format_desc(x))
    print("Description format changed")
    # transform smoking to boolean
    dataset_df['smoking'] = dataset_df['smoking'].apply(lambda x:format_smoking(x))
    print("Smoking preference format changed")
    # if parking place type column is in the dataset transform it's format
    if 'parking' in dataset_df.columns:
        dataset_df['parking'] = dataset_df['parking'].apply(lambda x:format_parking(x))
        print("Parking data format changed")
    # transform animals to boolean
    dataset_df['animals'] = dataset_df['animals'].apply(lambda x:format_animals(x))
    print("Animals allowed format changed")
    # transform flat type format
    dataset_df['type'] = dataset_df['type'].apply(lambda x:format_type(x))
    print("Type format changed")
    # convert flat/room size to float
    dataset_df['size_m2'] = dataset_df['size_m2'].apply(lambda x:format_size(x))
    print("Size m^2 format changed")
    # transform room/flat sharing data format
    if 'share' in dataset_df.columns:
        dataset_df['share'] = dataset_df['share'].apply(lambda x:format_sharing(x))
        print("Sharing format changed")    
    return dataset_df
    
def filter_dataset(dataset_df, price_limit=15000.0):
    """Removes rows with missing values in 'price' and 'date' columns. For
    rooms data additionaly removes rows with missing values in 'share' column.
    Removes ads with price higher than price_limit to remove outliers"""
    # remove rows without pirce data
    filtered_px = dataset_df.dropna(subset=['price'])
    # remove rows with price higher tnat 'price limit'
    filtered_px = filtered_px[filtered_px['price']<price_limit]
    # remove rows without date
    filtered_dt = filtered_px.dropna(subset=['date'])
    # for rooms: remove rows without data in 'share' column
    if 'share' in dataset_df.columns:
        filtered_sh = filtered_dt.dropna(subset=['share'])
        print("NA values removed from columns: 'price', 'date', 'share'")
        return filtered_sh
    # for flats:
    else:
        print("NA values removed from columns: 'price', 'date'")
        return filtered_dt


    

def find_street(desc):
    search_words = ['ulicy', 'ul.', 'ul', 'ulica', 'aleja', 'aleje', 'alei',
                    'plac', 'placu', 'Ul.', 'Ulica', 'Ulicy', 'Aleja', 'Aleje',
                    'Alei', 'Plac', 'Placu', 'alejach', 'Alejach']
    try:
        found_streets = []
        for word in search_words:
            pattern = r"(?<=\b{}\s)(\w+)".format(word) # next word
            found = re.findall(pattern, desc)
            if len(found)>0:
                for item in found:
                    found_streets.append(item)
            pattern = r"(?<=\b{}\s)(\w+)(\s)(\w+)".format(word) # next two words
            found = re.findall(pattern, desc)
            if len(found)>0:
                for item in found:
                    joined = ' '.join(item)
                    splitted = joined.split()
                    found_streets.append(splitted)
        if len(found_streets)>0:
            print(found_streets)
            return found_streets
        else:
            return None
    except TypeError:
        return None
    


def find_charges(description):
    """Function finds words "opłaty" and "ryczałt" in the description text to
    capture information about the full price"""
    search_words = ["ryczałt", "Ryczałt" "opłaty", "Opłaty" "opłat", "Opłat",
                    "kosztów", "Kosztów", "liczniki", "Liczniki", "liczników",
                    "Liczników", "licznikami", "opłatami", "Opłatami", "dopłaty",
                    "ryczałtu", "rachunki", "Rachunki", "media", "Media",
                    "oplaty", "Oplaty"]
    found = []
    for word in search_words:
        try:
            result = re.findall(r"[^.]*{}[^.]*\.............".format(word), description)
            if len(result)>0:
                found.append(result[0].strip())
        except TypeError:
            print("Incorrect description type")
            pass
    return list(set(found))


#
#
#ads_data_final = {}
#
#for k, v in ads_data_filtered.items():
#    if find_street(v["Opis"]) != []:
#        ads_data_final[k] = v
#        ads_data_final[k]["Ulica_re"] = find_street(v["Opis"])
#        ads_data_final[k]['Lokalizacja'] = remove_polish(ads_data_final[k]['Lokalizacja'])
#        if len(find_charges(v["Opis"]))>0:
#            print(find_charges(v["Opis"]))
#            ads_data_final[k]['Opłaty'] = find_charges(v["Opis"])
#    else:
#        ads_data_final[k] = v
#        ads_data_final[k]['Lokalizacja'] = remove_polish(ads_data_final[k]['Lokalizacja'])
#
#json_save(ads_data_final, "rooms_ads_filtered")
#
