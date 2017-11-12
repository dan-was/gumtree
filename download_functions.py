# -*- coding: utf-8 -*-
"""
Created on Sat Sep 23 23:38:30 2017

@author: Daniel
"""
import requests
from bs4 import BeautifulSoup
import json


def download_ad_links(page, ad_type):
    """Downloads and returns list of links from a page of ads for three types of
    ads: 
        Rent : 'flat' or 'room' 
        Sell : 'flat_sale
    """
    # specify link structure based on ad_type
    if ad_type=='flat':
        link = "https://www.gumtree.pl/s-mieszkania-i-domy-do-wynajecia/warszawa/mieszkanie/page-{}/v1c9008l3200008a1dwp{}".format(page, page)
    elif ad_type=='room':
        link = "https://www.gumtree.pl/s-pokoje-do-wynajecia/warszawa/page-{}/v1c9000l3200008p{}".format(page, page)
    elif ad_type=='flat_sale':
        link = "https://www.gumtree.pl/s-mieszkania-i-domy-sprzedam-i-kupie/warszawa/mieszkanie/page-{}/v1c9073l3200008a1dwp{}".format(page,page)
    # send a http get request
    req = requests.get(link)
    # store the html soure code in BS objct 
    soup = BeautifulSoup(req.content, "lxml")
    # find all the links in the source code
    links_content = soup.find_all("a", {"class": "href-link"}, href=True)
    # convert links to string a store them in a list
    links = [str(link['href']) for link in links_content]
    # remove duplicates
    links = list(set(links))
    #print("Downloaded page {}".format(page))
    return links

def download_ad_data(link):
    """Returns a dictionary with ad data from gumtree for a given link"""
    attr_dict = {}  #an empty dictionray where the data fill be stored
    try:
        # send a http get request (stop if no bytes received after 2 seconds)
        req = requests.get("https://www.gumtree.pl" + link,timeout=2)
        # store the html soure code in BS objct
        soup = BeautifulSoup(req.content, "lxml")   
        # separate the class that c1ontains atributes and description    
        content = soup.find_all("div", {"class": "vip-header-and-details"})[0]
        # find attribute keys
        keys_raw = content.find_all("span", {"class": "name"}) 
        # find attribute values
        values_raw = content.find_all("span", {"class": "value"})
        # convert to strings and remove unnecesary spaces and newlines
        atr = [str(attribute.text).strip() for attribute in keys_raw]
        val = [str(value.text).strip() for value in values_raw]
        # add attribute:value pairs in a dictionary
        for i in range(len(atr)):
            attr_dict[atr[i]] = val[i+1]        
        # find the ad desctiption
        desc_raw = content.find_all("div", {"class": "description"})
        # convert to string and remove unnecesary spaces and newlines
        desc = [str(value.text).strip() for value in desc_raw]
        # add the desctiption to the dictionary
        attr_dict["Opis"] =  desc[0]
        # find a class with adress data
        address_raw = soup.find_all("span", {"class": "address"})
        # convert to string and remove unnecesary spaces and newlines
        address = [str(item.text).strip() for item in address_raw]
        # add the adress to the dictionary
        attr_dict["Adres"] = address
        # if location was among atrributes remove all but the first item (district)
        if "Lokalizacja" in attr_dict:
            attr_dict["Lokalizacja"] = attr_dict["Lokalizacja"].split(",")[0]
        try:
            # delete currency symbol and convert to a float
            attr_dict["Cena"] = float(val[0][:-2].replace(u'\xa0', '')) 
        except:
            pass
        # add the ad's link to the dict
        attr_dict["link"] = "https://www.gumtree.pl" + link
        return attr_dict
    except:
        pass

def find_last_page(ad_type,page=1500):
    """Function to find the last page in either 'room' or 'flat' ads list
    
    Parameters
    ----------
    ad_type : str,
        'flat', 'room', 'flat_sale'
        
    page : int,
        page number that is certaintly after the last one - (as of 2017-09 - 
        apartments have around 600 and rooms 90 pages of ads so 1000 seems to be
        reasonable)
    """
    # determine the link to follow
    if ad_type=='flat':
        link = "https://www.gumtree.pl/s-mieszkania-i-domy-do-wynajecia/warszawa/mieszkanie/page-{}/v1c9008l3200008a1dwp{}".format(page, page)
    elif ad_type=='room':
        link = "https://www.gumtree.pl/s-pokoje-do-wynajecia/warszawa/page-{}/v1c9000l3200008p{}".format(page, page)
    elif ad_type=='flat_sale':
        link = "https://www.gumtree.pl/s-mieszkania-i-domy-sprzedam-i-kupie/warszawa/mieszkanie/page-{}/v1c9073l3200008a1dwp{}".format(page,page)
    # send a http get request
    req = requests.get(link)
    # store the html soure code in BS objct
    soup = BeautifulSoup(req.content, "lxml")
    # separate the class that c1ontains number of the last page 
    content = soup.find_all("span", {"class": "current"})
    # return the last page number as int
    return int(content[0].text)

def find_coordinates(street_name, key, city='warszawa'):
    """The function uses google geocoding api to find coordinates of a given
    adress (street with number in a specified city). The google API key must be
    given as an argument for the function to work"""
    link = "https://maps.googleapis.com/maps/api/geocode/json?address={},+{}&key={}".format(street_name,city,key)
    # make a get rquest to download the location data in json format
    req = requests.get(link, timeout=5.0)
    try:
        # extract the coordinates from the downloaded json file
        latitude = json.loads(req.text)["results"][0]["geometry"]["location"]["lat"]
        longitude = json.loads(req.text)["results"][0]["geometry"]["location"]["lng"]
        #print((latitude, longitude))
        return (latitude, longitude)
    except IndexError:
        return None