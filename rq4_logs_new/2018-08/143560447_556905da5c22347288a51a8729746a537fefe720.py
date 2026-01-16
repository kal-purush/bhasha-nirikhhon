import pandas as pd
import os
from AnalysisPackage import *
from filter_class import *
        
# -*- coding: utf-8 -*-
class filterDemo:
    def __init__(self):
        self.i = 0
    
    def Launch(self):

        os.chdir('/Users/chloe/Downloads/Booking_Analysis_Project-master/Dataset')
        nyc = pd.read_csv('nyc_all_newLabel.csv')  #  "_all" csv
        la = pd.read_csv('la_all_newLabel.csv')
        ol = pd.read_csv('ol_all_newLabel.csv')
        
        os.chdir('/Users/chloe/Desktop/UVa/Courses/CS5010/Project/dataset/senti_keyword')
        nyc_senti = pd.read_csv('nyc_senti.csv') # "_senti" csv
        la_senti = pd.read_csv('la_senti.csv')
        ol_senti = pd.read_csv('ol_senti.csv')
        nyc_all = merge_with_senti(nyc, nyc_senti)
        la_all = merge_with_senti(la, la_senti)
        ol_all = merge_with_senti(ol, ol_senti)
        
        nyc_keyword = pd.read_csv('nyc_5keywords.csv') # "_keyword' csv
        la_keyword = pd.read_csv('la_5keywords.csv')
        ol_keyword = pd.read_csv('ol_5keywords.csv')
        
        
        # ===================== Test Class ===================#
        
        user_city = input("Hello! Which city are you going to? NYC, Los Angeles or Orlando?\n")
        if user_city == "NYC":
            search = Filter(nyc_all,nyc_keyword)
        elif user_city == "Los Angeles":
            search = Filter(la_all,la_keyword)
        elif user_city == "Orlando":
            search = Filter(ol_all,ol_keyword)
        
        
        top = search.user_input()
        display_table(top)
        
        number = int(input("Which hotel do you prefer? 0, 1 or 2? \n"))
        hotel_name = top.iloc[number]['name']
        print (hotel_name + " is a great choice!")
        dic = get_distance(hotel_name, search.df)
        top3 = sorted(dic.items(), key=lambda x: x[1])[1:4]  # Another 3 hotels for recommendation
        print('')
        print ("Additional 3 recommendations for you: \n")
        print ("1." + top3[0][0])
        print ("2." + top3[1][0])
        print ("3." + top3[2][0])
        #    {}, {}, and {}.".format(top3[0][0], top3[1][0],top3[2][0]))
        choice = search.df[search.df['name'] == hotel_name]
        rec = search.df.loc[search.df['name'].isin([top3[0][0],top3[1][0],top3[2][0]])]
        rec_all = pd.concat([choice,rec])
        display_table(rec_all)
        print_fac(rec_all)
        
        