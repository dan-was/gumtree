# -*- coding: utf-8 -*-
"""
Created on Sat Sep 23 14:47:00 2017

@author: Daniel
"""

import concurrent.futures as cf
from tqdm import tqdm
from useful_functions import json_load, json_save
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
        
class Ads():
    """A Class that contains methods to update flats and rooms ads stored in 
    files ads_data.json (for flats) and ads_data_rooms (for rooms). In order to
    update the datasets make sure both files are in the same directory as this 
    script. Create an instance of the class for both flats and rooms and use method
    'download_new_ads in order to update them.
            
    Parameters
    ----------
    ad_type : string,
        'flat' or 'room'


    Examples
    --------
    >>> flats = Ads('flat')
    >>> flats.download_new_ads()

    """
    def __init__(self,ad_type):
        # determine the type of ad either 'flat' or 'room'
        self.type = ad_type
        if self.type=='flat':
            self.ads_data = json_load('ads_data.json') # load flats data
        elif self.type=='room':
            self.ads_data = json_load('ads_data_rooms.json') # load rooms data
        elif self.type=='flat_sale':
            self.ads_data = json_load('ads_data_sale.json') # load data of flats for sale
        # create a list of ad ids already in the dataset to avoid downloading again
        self.downloaded_ids = set([k for k,v in self.ads_data.items()])
        # empty list to store links to ads not yet included in the dataset
        self.new_links = []
    
    def __str__(self):
        try:
            return self.description
        except:
            return "Use 'filter_and_transform_to_df' method first"
    def __repr__(self):
        try:
            return self.description
        except:
            return "Use 'filter_and_transform_to_df' method first"
    
    def save_dataset(self):
        # determine the name of file where to store the data
        if self.type=='flat':
            filename = 'ads_data'
        elif self.type=='room':
            filename = 'ads_data_rooms'
        elif self.type=='flat_sale':
            filename = 'ads_data_sale'
        # save the updated dataset in a json file
        json_save(self.ads_data, filename)

    def download_new_ads(self, max_pages='all'):
        """Update current dataset with new ads form gumtree.pl
        
        Parameters
        ----------
        max_pages : int or str,
            {default : str : 'all_pages' will check how many pages of ads are available},
            {int : explicitly pass int of number of pages to check}
        """
        import download_functions as dwnl
        # check the number of last page with ads and download links from all pages
        if max_pages == 'all':
            max_pages = dwnl.find_last_page(self.type)
        print('')
        # progress bar variable to keep track of progress
        t = tqdm(total=max_pages,desc='Downloading {} links'.format(self.type))
        def download_links(page):
            """additional function to pass ad type and add progress barr"""
            data = dwnl.download_ad_links(page=page,ad_type=self.type)
            # update progress by 1
            t.update()
            return data
        # multithreaded (asynchronous) requests sent to webstie to increase
        # download speed
        with cf.ThreadPoolExecutor(max_workers=10) as pool:
            link_results = pool.map(download_links, range(1,max_pages+1))
        list_version = list(link_results) # convert results into list
        links_set = set() # empty set to remove potential duplicates
        # add downloaded links to the links set
        for item in list_version:
            for link in item:
                links_set.add(link)
        # convert links set to a list
        links = list(links_set)
        # if ad_id not in already downloaded ids add the link to new_links list
        for link in links:
            if link.split('/')[-1] not in self.downloaded_ids:
                self.new_links.append(link)
        print('')
        # second progress bar variable for ads data
        p = tqdm(total=len(self.new_links),desc='Downloading {} ads'.format(self.type))
        def download_ads(link):
            """additional function to add progress bar and update downloaded_ids
            list with ids of new ads"""
            ad = dwnl.download_ad_data(link)
            # update progress by 1
            p.update()
            # add id of the downloaded ad to the list
            self.downloaded_ids.add(link.split('/')[-1])
            return ad
        # multithreaded (asynchronous) requests sent to webstie to increase
        # download speed
        with cf.ThreadPoolExecutor(max_workers=10) as pool:
            ad_results = pool.map(download_ads, self.new_links,timeout=10)
        # convert results to a list
        list_version = list(ad_results)
        # create a class atribute with the list of new ads data
        self.new_ads = list_version
        print('')
        print("Download completed")
        n_new_ads = 0 # number of new ads added to the dataset
        for ad in self.new_ads:
            # check if the ad is not empty (in case of a download error)
            if ad!=None:
                ad_id = ad['link'].split('/')[-1]
                # add the data of new ad to the dataset
                self.ads_data[ad_id] = ad
                n_new_ads += 1
        # determine the name of file where to store the data
        if self.type=='flat':
            filename = 'ads_data'
        elif self.type=='room':
            filename = 'ads_data_rooms'
        elif self.type=='flat_sale':
            filename = 'ads_data_sale'
        # save the updated dataset in a json file
        json_save(self.ads_data, filename)
        print('New {} ads downloaded: {}'.format(self.type,n_new_ads))
        print('Updated {}.json saved'.format(filename))
        
    def filter_and_transform_to_df(self, price_limit, size_limit):
        """Method filters the dataset by removing all observations with
        incorrect data format and with price and size higher than specified
        limits. The dataset is then transformed into a pandas DataFrame named
        'filtered_data, which is ready for further analysis"""
        from filtering_functions import transform_dataset, filter_dataset
        # transform the dataset to dataframe (and change format of variables)
        # and filter unwanted data
        self.filtered_data = filter_dataset(transform_dataset(self.ads_data),
                                            price_limit,size_limit)
        # create an easily accesible description of the resulting dataset
        n, k = self.filtered_data.shape
        self.description = "Ads dataset class. Number of observations: {}, Number of variables: {}".format(n,k)
        
    def find_street_in_descriprion(self):
        """Method uses regular expressions to find more accurate adress of the
        offered flat/room in ad's description"""
        from filtering_functions import find_street
        for ad_id, ad_data in self.ads_data.items():
            if 'Ulica_re' not in ad_data.keys():
                self.ads_data[ad_id]['Ulica_re'] = find_street(self.ads_data[ad_id]['Opis'])
    
    def ad_number_ts(self):
        """Simple time series countplot showing number of ads added on a given
        day. Under development"""
        piv = self.filtered_data.pivot_table(values='price',index='date',aggfunc='count')
        piv.plot()
    
    def subset(self, last_n_dates=3, location='any', n_rooms='any', agency=False):
        """Method returns a subset of ads that meet the specified criteria:
        
            Parameters
            ----------
        
            last_n_dates : int
                number of days in the past to include
            
            location : list of strings or string 'any'
                list of city districts to include
            
            n_romms : list of ints or string 'any'
                list of integers indicating number of rooms in a flat        

            agency : bool
                specify if ads posted by agencies should be included, default 'False'
        """
        # create a copy of the dataset
        try:
            dat = self.filtered_data.copy()
        except NameError:
            print("No dataset found, use 'filter_and_transform_to_df' method first")
        # filter location if a list of districts specified
        if location != 'any':
            df_list = []
            for district in location:
                # append a dataframe with ads for specified district
                df_list.append(dat[dat['location']==district])
            # merge all dataframes into one
            dat = pd.concat(df_list)
        # filter number of rooms if a list specified
        if n_rooms != 'any':
            df_list = []
            for n in n_rooms:
                # append a dataframe with ads with specific number of rooms
                df_list.append(dat[dat['n_rooms']==n])
            # merge all dataframes into one
            dat = pd.concat(df_list)
        df_list = []
        td = datetime.date.today() # datetime object with today's date
        delta = datetime.timedelta(1)  # step (delta) size (one day difference)
        for d in range(last_n_dates,0,-1): # loop from the last day
            # add ads added on a specific day to the list
            df_list.append(dat[dat['date']==td])
            td -= delta
        # merge all dataframes into one    
        dat = pd.concat(df_list)
        # remove ads added by agencies
        if not agency:
            dat = dat[dat['advertiser']=='owner']
        return dat

    def download_coords(self,key,override=False):
        """Method downloads coordindates of flat/room based on the data in 'adress' 
        column using google geocoding API and saves them as a dictionary in json file
        
        Format:
            ad_id : [latitute,longitude]

        Requires an API key to work
        """
        from download_functions import find_coordinates
        # list of ad ids in the filtered dataset
        ids = list(self.filtered_data.index)
        # load already downloaded coordinades dataset
        coords = json_load('coords.json')
        n = 0 # count number of api requests
        for ad_id in ids:
            if ad_id not in coords.keys():
                if n < 2500:  # stop when limit od 2,5k daily requests is met
                    print(ad_id)
                    coords[ad_id] = find_coordinates(self.filtered_data['adress'].loc[ad_id],key)
                    n +=1
                    print(n)
                    if n%100==0:
                        json_save(coords, 'coords') # save every 100 downloaded coords
        # remove None values
        coords = dict((k,v) for k,v in coords.items() if v is not None)
        # save updated coords set in a json file
        json_save(coords, 'coords')
    
    def load_coords(self):
        """Load coords from json file"""
        cdrs = json_load('coords.json')
        ids = list(self.filtered_data.index)
        init_coords = []
        for ad_id, crd in cdrs.items():
            if ad_id in ids:
                init_coords.append([ad_id,crd[0],crd[1],self.filtered_data['price'].loc[ad_id],
                                    self.filtered_data['size_m2'].loc[ad_id]])
        filtered = []
        for item in init_coords:
            if item[1] >52.10 and item[1] <52.4 and item[2]>20.8 and item [2] <21.4:
                filtered.append(item)
        self.coords = filtered
        
    def coords_plot(self):
        """Create a colormap of flats based no prace per square meter - under development
        """
        self.load_coords()
        x = []
        y = []
        px = []        
        for item in self.coords:
            if item[1] >52.10 and item[1] <52.4 and item[2]>20.8 and item [2] <21.4:
                x.append(item[1])
                y.append(item[2])
                px.append(item[3])
        plt.scatter(x,y,c=px,s=150,alpha=0.3)
        plt.show()
        
            

if __name__=='__main__':
    flats_rent = True
    rooms_rent = True
    flats_sale = True
    
    download_new = True
    transform = False
    
    if flats_rent:
        flats = Ads('flat')
        if download_new:
            flats.download_new_ads()
            flats.find_street_in_descriprion()
            flats.save_dataset()
        if transform:
            flats.filter_and_transform_to_df(price_limit=15000.0,size_limit=300.0)
            dataset_flats = flats.filtered_data
        
    if rooms_rent:
        rooms = Ads('room')
        if download_new:
            rooms.download_new_ads()
            rooms.find_street_in_descriprion()
            rooms.save_dataset()
        if transform:
            rooms.filter_and_transform_to_df(price_limit=15000.0,size_limit=300.0)
            dataset_rooms = rooms.filtered_data
        
    if flats_sale:
        flats_sale = Ads('flat_sale')
        if download_new:
            flats_sale.download_new_ads()
            flats_sale.find_street_in_descriprion()
            flats_sale.save_dataset()
        if transform:
            flats_sale.filter_and_transform_to_df(price_limit=10000000.0,
                                                  size_limit=300.0)
            dataset_flats_sale = flats_sale.filtered_data