# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 21:47:02 2017

@author: Daniel
"""

import pickle
import json
import unicodedata

def pickle_load(file):
    with open(file, 'rb') as f:
        variable = pickle.load(f)
    return variable

def pickle_save(variable, filename):
    with open(str(filename + '.pickle'), 'wb') as f:
        pickle.dump(variable, f)
    print('File ' + str(filename) + ' saved')
    
def json_load(file):
    """Loads a json file from the current directory
    DON'T SPECIFY THE EXTNESION"""
    with open(file, 'r') as f:
        variable = json.load(f)
    return variable

def json_save(variable, filename, quiet = True):
    "Include extenstion! Saves a dict to json"
    with open(str(filename), 'w') as f:
        json.dump(variable, f, sort_keys=True, indent=4)
    if not quiet:
        print('File ' + str(filename) + ' saved')
    
def remove_polish(string):
    """Funkcja zastepuje polskie znaki ich miedzynarodowymi odpowiednikami"""
    try:
        return unicodedata.normalize('NFKD', string).replace(u'ł', 'l').encode('ascii', 'ignore').decode("utf-8") 
    except:
        print(string)