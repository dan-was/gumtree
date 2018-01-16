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
    
    filename : string, None
        Specify the name of file you want to load the data from. If file is found
        it will be loaded, otherwise a new JSON file will be created


    Examples
    --------
    >>> flats = Ads('flat', 'ads_data.json')
    >>> flats.download_new_ads()

    """
    def __init__(self,ad_type, filename):
        # determine the type of ad either 'flat' or 'room'
        self.type = ad_type
        self.filename = filename
        try:
            self.ads_data = json_load(filename)
        except FileNotFoundError:
            print("File {} not found. New file will be created on save".format(filename))
            self.ads_data = dict()
        # create a list of ad ids already in the dataset to avoid downloading again
        self.downloaded_ids = set([k for k,v in self.ads_data.items()])
        # empty list to store links to ads not yet included in the dataset
        self.new_links = []
        # an empty dictionary to store price prediction models
        self.models = {}
    
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
        # save the updated dataset in a json file
        json_save(self.ads_data, self.filename)

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
        # save the updated dataset in a json file
        self.save_dataset()
        print('New {} ads downloaded: {}'.format(self.type,n_new_ads))
        print('Updated {} saved'.format(self.filename))
        
    def filter_and_transform_to_df(self, min_price, max_price, size_limit):
        """Method filters the dataset by removing all observations with
        incorrect data format and with price and size higher than specified
        limits. The dataset is then transformed into a pandas DataFrame named
        'filtered_data, which is ready for further analysis"""
        from filtering_functions import transform_dataset, filter_dataset
        # transform the dataset to dataframe (and change format of variables)
        # and filter unwanted data
        self.filtered_data = filter_dataset(transform_dataset(self.ads_data),
                                            min_price,max_price,size_limit)
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
                        json_save(coords, 'coords.json') # save every 100 downloaded coords
        # remove None values
        coords = dict((k,v) for k,v in coords.items() if v is not None)
        # save updated coords set in a json file
        json_save(coords, 'coords.json')
    
    def load_coords(self):
        """Load coords from json file"""
        # load json file that contains dictionary with ad IDs and coordinates
        cdrs = json_load('coords.json')
        # create a list of IDs in the contained in the dataset
        ids = list(self.filtered_data.index)
        init_coords = []    # a list of coords before filtering
        for ad_id, crd in cdrs.items():
            if ad_id in ids:
                # add [id,latitute,longitude,price,size]
                init_coords.append([ad_id,crd[0],crd[1],self.filtered_data['price'].loc[ad_id],
                                    self.filtered_data['size_m2'].loc[ad_id]])
        filtered = []   # empty list for filtered coords (removing coords outside Warsaw)
        for item in init_coords:
            if item[1] >52.10 and item[1] <52.4 and item[2]>20.8 and item [2] <21.4:
                filtered.append(item)
                # save as a class variable
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
        
    def LinReg(self, district):
        """Estimates parameters of a linear regression model based on data for
        a given district. Returns an instance of LinearModel object that can
        be used to predict values of out-of-sample data"""
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import LinearRegression
        # same variables used in the model for flat and flat sale
        if self.type=='flat_sale' or self.type=='flat':
            try:
                # load transformed dataser
                dataset = self.filtered_data
                # default values for NA
                values = {'n_bath': 1.0, 'parking': 'no'}
                # fill missing values with defaults
                dataset = dataset.fillna(value=values)
                # create a subset of variables that will be used to estimate the moddel
                dataset = dataset[['price','n_rooms','size_m2','parking','n_bath','location']]
                # drop remaining observations with missing values
                dataset.dropna(inplace=True)
                # create a subset of observations for a selected district
                district_data = dataset[dataset['location'] == district]
                # drop location column (as it contins one value after filtering)
                district_data = district_data[['price','n_rooms','size_m2','parking','n_bath']]
                # create dummy columns for four types of parking place
                # ['garage','basement','street','no']
                parking = pd.get_dummies(district_data['parking'],prefix='parking')
                # add parking variables to the dataset
                model_dataset = pd.concat([district_data,parking],axis=1)
                # drop original parking variable as well as one indicating lack
                # of parking place to prevent multicollinerality
                model_dataset.drop(['parking','parking_no'],axis=1,inplace=True)
                # create input (X) and target (y) variable vecotrs
                X = model_dataset.drop('price',axis=1)
                y = model_dataset['price']
                # spit the dataset into training and testing data
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
                # create an instance of Linear Model and estimate the parameters
                lm = LinearRegression()
                lm.fit(X_train,y_train)
                # print n_obs, R^2 metric and return the model object
                print("Model evaluated, n = {}, r^2 = {}".format(len(model_dataset),
                                                          round(lm.score(X_test,y_test),3)))
                return lm
            except AttributeError:
                print("Dataset not transformed yet, use method 'filter_and_transform_to_df' first")
                
        
    def predict_price_from_link(self,link):
        """Downloads ad information from a given link and based on the variable
        values uses an estimated linear regression model to predict price"""
        import download_functions as dwnl
        from filtering_functions import create_dataframe_from_one_ad
        ad_dict = dwnl.download_ad_data(link)
        df, district = create_dataframe_from_one_ad(ad_dict)
        input_variables = df.drop('price',axis=1)
        print(input_variables.loc[0].transpose())
        print(" ")
        try:
            lm = self.models[district]
        except KeyError:
            lm = self.LinReg(district)
            self.models[district] = lm
        pred = lm.predict(input_variables)
        print(" ")
        print('Estimated price: {0:.2f}'.format(float(pred)))
