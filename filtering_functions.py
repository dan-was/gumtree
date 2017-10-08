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
            ['adress', 'price', 'date', 'advertiser', 'available', 'n_rooms',
            'location', 'desc', 'charges', 'smoking', 'pref_gen', 'animals', 'type',
            'street_regex', 'size_m2', 'share', 'url']
                
    Flats_sale:
            ['adress', 'price', 'date', 'n_rooms', 'n_bath', 'location', 
            'advertiser', 'desc', 'parking', 'type', 'size_m2', 'url',
            'street_regex']
    
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
                 'Na sprzedaż przez':'advertiser',
                 'Dostępny':'available', 'Liczba pokoi':'n_rooms',
                 'Liczba łazienek':'n_bath', 'Lokalizacja':'location',
                 'Opis':'desc', 'Palący':'smoking', 'Parking':'parking',
                 'Przyjazne zwierzakom':'animals', 'Rodzaj nieruchomości':'type',
                 'Ulica_re':'street_regex', 'Wielkość (m2)':'size_m2',
                 'link':'url','Opłaty':'charges','Preferowana płeć':'pref_gen',
                 'Współdzielenie':'share'}
    # transform the dict dataset to pandas DataFrame object with ads as rows
    dataset_df = pd.DataFrame(dataset_dict).transpose()
    # change column names
    dataset_df.columns = list(map(lambda x:colnames_dict[x], list(dataset_df.columns)))
    # transform adress
    dataset_df['adress'] = dataset_df['adress'].apply(lambda x:format_adress(x))
    # convert pirce
    dataset_df['price'] = dataset_df['price'].apply(lambda x:format_price(x))
    # convert ad date and available date to datetime format
    dataset_df['date'] = dataset_df['date'].apply(lambda x:format_date(x))
    if 'available' in dataset_df.columns:
        dataset_df['available'] = dataset_df['available'].apply(lambda x:format_date(x))
    # transform advertiser type
    dataset_df['advertiser'] = dataset_df['advertiser'].apply(lambda x:format_advertiser(x)) 
    # convert number of rooms to float
    dataset_df['n_rooms'] = dataset_df['n_rooms'].apply(lambda x:format_n_rooms(x))
    # if number of bathrooms column is in the dataset convert it to float
    if 'n_bath' in dataset_df.columns:
        dataset_df['n_bath'] = dataset_df['n_bath'].apply(lambda x:format_n_bathrooms(x))
    # transform location format
    dataset_df['location'] = dataset_df['location'].apply(lambda x:format_location(x))
    # transform description format
    dataset_df['desc'] = dataset_df['desc'].apply(lambda x:format_desc(x))
    # transform smoking to boolean
    if 'smoking' in dataset_df.columns:
        dataset_df['smoking'] = dataset_df['smoking'].apply(lambda x:format_smoking(x))
    # if parking place type column is in the dataset transform it's format
    if 'parking' in dataset_df.columns:
        dataset_df['parking'] = dataset_df['parking'].apply(lambda x:format_parking(x))
    # transform animals to boolean
    if 'animals' in dataset_df.columns:
        dataset_df['animals'] = dataset_df['animals'].apply(lambda x:format_animals(x))
    # transform flat type format
    dataset_df['type'] = dataset_df['type'].apply(lambda x:format_type(x))
    # convert flat/room size to float
    dataset_df['size_m2'] = dataset_df['size_m2'].apply(lambda x:format_size(x))
    # transform room/flat sharing data format
    if 'share' in dataset_df.columns:
        dataset_df['share'] = dataset_df['share'].apply(lambda x:format_sharing(x))
    print("Dataset transfromed to a DataFrame and formatted")
    return dataset_df
    
def filter_dataset(dataset_df, price_limit, size_limit):
    """Removes rows with missing values in 'price' and 'date' columns. For
    rooms data additionaly removes rows with missing values in 'share' column.
    Removes ads with price higher than price_limit to remove outliers"""
    # remove rows without pirce data
    filtered_px = dataset_df.dropna(subset=['price'])
    # remove rows with price higher tnat 'price limit'
    filtered_px = filtered_px[filtered_px['price']<price_limit]
    # remove rows with size higher than 'size limi'
    filtered_size = filtered_px[filtered_px['size_m2']<size_limit]
    # remove rows without date
    filtered_dt = filtered_size.dropna(subset=['date'])
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
    """Function uses regular expressions to find the street name and number in
    the description"""
    # list of key words afret which street name is likely to be mentioned
    search_words = ['ulicy', 'ul.', 'ul', 'ulica', 'aleja', 'aleje', 'alei',
                    'plac', 'placu', 'Ul.', 'Ulica', 'Ulicy', 'Aleja', 'Aleje',
                    'Alei', 'Plac', 'Placu', 'alejach', 'Alejach']
    try:
        # an empty list where all search results will be stored
        found_streets = []
        for word in search_words:
            # pattern to match one word that appears after 'word'
            pattern = r"(?<=\b{}\s)(\w+)".format(word) # next word
            found = re.findall(pattern, desc)
            if len(found)>0:
                for item in found:
                    # add all matches to the list
                    found_streets.append(item)
            # pattern to match two words that apperar after 'word'. Added to
            # adress double-word street names problem
            pattern = r"(?<=\b{}\s)(\w+)(\s)(\w+)".format(word) # next two words
            found = re.findall(pattern, desc)
            if len(found)>0:
                for item in found:
                    # joins and splits to keep consistency and create a list
                    # with distictive words and numbers as separate elements
                    joined = ' '.join(item)
                    splitted = joined.split()
                    # add all matches to the list
                    found_streets.append(splitted)
        if len(found_streets)>0:
            # return list of lists with matches if anything found
            return found_streets
        else:
            return None
    except TypeError:
        print("Invalid description type")
        return None
    


def find_charges(description):
    """Function finds words from 'search_words' list in the description text to
    capture information about additional charges/bills"""
    search_words = ["ryczałt", "Ryczałt" "opłaty", "Opłaty" "opłat", "Opłat",
                    "kosztów", "Kosztów", "liczniki", "Liczniki", "liczników",
                    "Liczników", "licznikami", "opłatami", "Opłatami", "dopłaty",
                    "ryczałtu", "rachunki", "Rachunki", "media", "Media",
                    "oplaty", "Oplaty"]
    # empty list to store all matches
    found = []
    try:
        for word in search_words:
            # match all sentences with 'word' and few characters after the dot
            result = re.findall(r"[^.]*{}[^.]*\.............".format(word), description)
            if len(result)>0:
                # if anything found return list of mathces
                found.append(result[0].strip())
        return list(set(found))
    except TypeError:
        print("Invalid description type")
        return None
    

