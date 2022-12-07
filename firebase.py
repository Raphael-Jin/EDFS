# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 16:46:26 2022

@author: susie
"""
from command.fscommand import *
username = 'jhadmin'
group = 'admin'

start()
mkdir('/movie',username,group) # success
mkdir('/user',username,group) # success
mkdir('/user/user_data',username,group)
mkdir('/user/user_data/history',username,group)

dir1 = '/movie'
fileloc1 = './data/movie_info.csv'
partition_num1 = 3
put(fileloc1,dir1,partition_num1) # success #no user group

dir2 = '/user/user_data/history'
file2 = './data/ratings.csv'
partition_num2 = 3
put(file2,dir2,partition_num2) # success5

mkdir('/user/user_data/custom',username,group)
mkdir('/user/user_data/custom/rating',username,group)

dir3 = '/user/user_data'
file3 = './data/user_info.csv'
partition_num3 = 1
put(file3,dir3,partition_num3) # success

put_efile('rating_10001.csv','/user/user_data/custom/rating',fsize=0,p=1,storecal=0,storeway="hashing",user ='user',group = 'group')
addpartition('/user/user_data/custom/rating/rating_10001.csv', {'userId':10001,'movieId':31,'rating':2.5,'timestamp':1260759144})

