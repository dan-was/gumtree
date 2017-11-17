# -*- coding: utf-8 -*-
"""
Created on Fri Nov 17 19:54:17 2017

@author: Daniel

Main script with standard methods of 'Ads' object used to download and transform
dataset

"""

from ads_tools import Ads


if __name__=='__main__':
    flats_rent = True
    rooms_rent = False
    flats_sale = False
    
    download_new = False
    transform = True
    
    if flats_rent:
        flats = Ads('flat')
        if download_new:
            flats.download_new_ads()
            flats.find_street_in_descriprion()
            flats.save_dataset()
        if transform:
            flats.filter_and_transform_to_df(min_price = 300.00,
                                             max_price=15000.0,
                                             size_limit=300.0)
            dataset_flats = flats.filtered_data
        
    if rooms_rent:
        rooms = Ads('room')
        if download_new:
            rooms.download_new_ads()
            rooms.find_street_in_descriprion()
            rooms.save_dataset()
        if transform:
            rooms.filter_and_transform_to_df(min_price = 300.00,
                                             max_price=15000.0,
                                             size_limit=300.0)
            dataset_rooms = rooms.filtered_data
        
    if flats_sale:
        flats_sale = Ads('flat_sale')
        if download_new:
            flats_sale.download_new_ads()
            flats_sale.find_street_in_descriprion()
            flats_sale.save_dataset()
        if transform:
            flats_sale.filter_and_transform_to_df(min_price = 30000.00,
                                                  max_price=10000000.0,
                                                  size_limit=300.0)
            dataset_flats_sale = flats_sale.filtered_data