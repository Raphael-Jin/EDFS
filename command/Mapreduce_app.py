from .Mapreduce import *
from .fscommand import *
import json
import collections
import ast

# filter + rank top N by rates (do not group)
def filter_rank_map(single_line: dict, args) -> dict:
    # args[0] -> movie type ; args[1] -> order by
    type = args[0]
    order_by = args[1]
    if type in single_line["genres"]:
        if order_by == "Popularity":
            # just rating
            # return {single_line["vote_average"]: [single_line["id"], single_line["title"], single_line["vote_count"]]}
            
            # using weighted_rating
            '''
            We can use the average ratings of the movie as the score but using this won't be fair enough 
            since a movie with 8.9 average rating and only 3 votes cannot be considered better than the 
            movie with 7.8 as as average rating but 40 votes. 
            '''
            v = single_line['vote_count']
            R = single_line['vote_average']
            # Calculation based on the IMDB formula
            '''
            v is the number of votes for the movie;
            m is the minimum votes required to be listed in the chart;
            R is the average rating of the movie;
            C is the mean vote across the whole report;
            '''
            # m= df2['vote_count'].quantile(0.9)
            m = 1838 # it must have more votes than at least 90% of the movies in the list.

            # C= df2['vote_average'].mean()
            C = 6.092171559442011
            return {(single_line["id"], single_line["title"]): ((v/(v+m) * R) + (m/(m+v) * C))}
        elif order_by == "Year":
            return {(single_line["id"], single_line["title"]): int(single_line["release_date"])}
    return None

def filter_rank_combine(map_result, args): # N -> select Top N
    desc, N = args[2], args[3]
    # args[2] -> desc, args[3] -> number of result
    combine_res = []
    for partition in map_result:
        # select TOP N
        # utilize heap sort
        import collections
        this_partition_combine_res = collections.defaultdict(list)
        for kv_pair in partition:
            this_partition_combine_res[list(kv_pair.items())[0][0]] = list(kv_pair.items())[0][1]

        import heapq
        if desc == True:
            this_partition_combine_res = \
                dict(heapq.nlargest(N, list(this_partition_combine_res.items()), key = lambda x: x[1]))
        else:
            this_partition_combine_res = \
                dict(heapq.nsmallest(N, list(this_partition_combine_res.items()), key = lambda x: x[1]))

        # this_partition_combine_res = \
        #     dict(sorted(this_partition_combine_res.items(), key = lambda x: x[1], reverse= True)[:N])

        combine_res.append(this_partition_combine_res)    # [[{}, {}], []]

    ### debug ouput ###
    print(combine_res[0])

    return combine_res

def filter_rank_reduce(combine_res, args):
    desc, N = args[2], args[3]
    # args[2] -> desc, args[3] -> number of result
    import collections
    reduce_res = collections.defaultdict(list)
    for each_partition_combine_res in combine_res:
        for k,v in each_partition_combine_res.items():
            reduce_res[k] = v
    
    # select TOP N
    if desc == True:
        reduce_res = dict(sorted(reduce_res.items(), key = lambda x:x[1], reverse= True)[:N])
    else:
        reduce_res = dict(sorted(reduce_res.items(), key = lambda x:x[1])[:N])
    
    ### debug ouput ###
    print(list(reduce_res.items())[0])

    return dict(reduce_res)

# filter_rank
def movie_filter_n_rank(movie_type = "Action", order_by = "Rating", N = 10, file = '/movie/movie_info.csv', Desc = True):
    args = [movie_type, order_by, Desc, N]
    map_res = mapPartition(file, filter_rank_map, args)
    reduce_res = reduce(map_res, filter_rank_combine, filter_rank_reduce, args)

    ans_list = []
    for k,v in reduce_res.items():
        id, title = k   # unpack
        ans_list.append([id, title, round(v,2)])

    import pandas as pd
    header = ["Movie ID", "Title", order_by]
    df = pd.DataFrame(ans_list, columns = header)
    df.index += 1
        
    return df


def name_search_map(single_line: dict, name: str) -> dict:
    if name.lower() in single_line["title"].lower(): # case insensitive
        return {single_line["id"] : [single_line["title"], single_line["genres"], single_line["release_date"], single_line["cast"]]}
    return None

def name_search_reduce(map_res):
    # map_res -> [[{}, {}, {}], [], []]
    reduce_res = []
    import ast
    for partition in map_res:
        for kv_pair in partition: # {}
            kv_2list = list(kv_pair.items())
            reduce_res.append([kv_2list[0][0], 
             kv_2list[0][1][0],
             ast.literal_eval(str(kv_2list[0][1][1])) if kv_2list[0][1][1][0] != "U" else kv_2list[0][1][1], 
             kv_2list[0][1][2],
             ast.literal_eval(str(kv_2list[0][1][3])) if kv_2list[0][1][3][0] != "U" else kv_2list[0][1][3]
             ])  # cast
    
    ### debug ouput ###
    print(reduce_res)

    return reduce_res

# movie searcher
def movie_searcher(movie_name, file = '/movie/movie_info.csv'):
    map_res = mapPartition(file, name_search_map, args=movie_name)
    reduce_res = reduce(map_res, combine_func= None, reduce_func=name_search_reduce, args = movie_name)

    import pandas as pd
    header = ["Movie ID", "Title", "Genres", "Year", "Cast"]
    df = pd.DataFrame(reduce_res, columns = header)
    df.index += 1
        
    return df


def cast_search_map(single_line: dict, name: str) -> dict:
    if single_line["cast"][0] != "[":
        return None

    cast_list = ast.literal_eval(single_line["cast"])    
    for actor in cast_list:
        if name.lower() in actor.lower(): # case insensitive
            return {single_line["id"] : [single_line["title"], single_line["genres"], single_line["release_date"], single_line["cast"]]}
    return None

def cast_search_reduce(map_res):
    # map_res -> [[{}, {}, {}], [], []]
    reduce_res = []
    import ast
    for partition in map_res:
        for kv_pair in partition: # {}
            kv_2list = list(kv_pair.items())
            reduce_res.append([kv_2list[0][0], 
             kv_2list[0][1][0],
             ast.literal_eval(str(kv_2list[0][1][1])) if kv_2list[0][1][1][0] != "U" else kv_2list[0][1][1], 
             kv_2list[0][1][2],
             ast.literal_eval(str(kv_2list[0][1][3])) if kv_2list[0][1][3][0] != "U" else kv_2list[0][1][3]
             ])  # cast
    
    ### debug ouput ###
    print(reduce_res)

    return reduce_res

# movie searcher
def cast_searcher(cast_name, file = '/movie/movie_info.csv'):
    map_res = mapPartition(file, cast_search_map, args=cast_name)
    reduce_res = reduce(map_res, combine_func= None, reduce_func=cast_search_reduce, args = cast_name)

    import pandas as pd
    header = ["Movie ID", "Title", "Genres", "Year", "Cast"]
    df = pd.DataFrame(reduce_res, columns = header)
    df.index += 1
        
    return df

def date_search_map(single_line: dict, time_range: list) -> dict:
    if time_range[0] <= single_line["release_date"] <= time_range[1]:
        return {single_line["id"] : [single_line["title"], single_line["genres"], single_line["release_date"], single_line["cast"]]}
    return None

def date_search_reduce(map_res):
    # map_res -> [[{}, {}, {}], [], []]
    reduce_res = []
    import ast
    for partition in map_res:
        for kv_pair in partition: # {}
            kv_2list = list(kv_pair.items())
            reduce_res.append([kv_2list[0][0], 
             kv_2list[0][1][0],
             ast.literal_eval(str(kv_2list[0][1][1])) if kv_2list[0][1][1][0] != "U" else kv_2list[0][1][1], 
             kv_2list[0][1][2],
             ast.literal_eval(str(kv_2list[0][1][3])) if kv_2list[0][1][3][0] != "U" else kv_2list[0][1][3]
             ])  # cast
    
    ### debug ouput ###
    print(reduce_res)

    return reduce_res


def date_searcher(user_input_time1, user_input_time2, file = '/movie/movie_info.csv'):
    map_res = mapPartition(file, date_search_map, args=[user_input_time1, user_input_time2])
    reduce_res = reduce(map_res, combine_func= None, reduce_func=date_search_reduce, args=[user_input_time1, user_input_time2])

    import pandas as pd
    header = ["Movie ID", "Title", "Genres", "Year", "Cast"]
    df = pd.DataFrame(reduce_res, columns = header)
    df.index += 1
        
    return df


# ---------- full_info_map --------------
def full_info_map(single_line: dict, ids) -> dict:
    if single_line["id"] in ids: # case insensitive
        return {single_line["id"]:single_line}
    return None

def full_info_reduce(map_res):
    # map_res -> [[{}, {}, {}], [], []]
    reduce_res = {}
    for partition in map_res:
        for kv_pair in partition: # {}
            reduce_res[list(kv_pair.items())[0][0]] = list(kv_pair.items())[0][1]
    return reduce_res

def full_info_checker(movie_ids, file = '/movie/movie_info.csv'):
    if type(movie_ids) != list:
        movie_ids = [movie_ids]

    map_res = mapPartition(file, full_info_map, args=movie_ids)
    reduce_res = reduce(map_res, combine_func= None, reduce_func=full_info_reduce, args = movie_ids)  
    return reduce_res

# --------avg calculator---------

def avg_map_func(single_line: dict) -> dict:
    if single_line["rating"] < 1:
        return {single_line["movieId"]: single_line["rating"]}
    return None

def avg_combine_func(map_result):
    # [[k:v,k:v],...., [k:v]]
    combine_res = []
    for partition in map_result:
        import collections
        this_partition_combine_res = collections.defaultdict(list)
        for kv_pair in partition:
            this_partition_combine_res[list(kv_pair.items())[0][0]].append(list(kv_pair.items())[0][1])
        for k,v in this_partition_combine_res.items():  # avg
            this_partition_combine_res[k] = round((sum(v) / len(v)), 1)
        combine_res.append(dict(this_partition_combine_res))
    return combine_res

def avg_reduce_func(combine_res):
    import collections
    reduce_res = collections.defaultdict(list)
    for each_partition_combine_res in combine_res:
        for k,v in each_partition_combine_res.items():
            reduce_res[k].append(v)
    for k,v in reduce_res.items():
        reduce_res[k] = round((sum(v) / len(v)), 1)
    return dict(reduce_res)


# -----------Item CF-------------
# step 1 group by user map 
def user_2_moive_map(single_line:dict, args=None):
    # map_res = userid : (movieid : movie rating)
    return (single_line["userId"],{single_line["movieId"]:single_line["rating"]})
def user_2_moive_reduce(map_res,args=None):
    # map_res = userid:(movieid : movie rating)
    # combine_res = {1:{101:5,102:3,103:2}, 2:[] ...}
    import collections
    reduce_res = collections.defaultdict(list)
    for partition in map_res:
        for kv_pair in partition: # {}
            reduce_res[kv_pair[0]].append(kv_pair[1])  # cast
    return dict(reduce_res)

def CartesianProduct_map(user_like:dict):
    # 1:[101:5,102:2,103:3] -> {"101":{"102": 1, "103":1}, "102":{"103": 1}}
    # set remove dupliecate
    movie_list = []    
    kv_pairs = list(user_like.items())[0][1]
    # a = [{31: 2.5},... ]
    # user_like_value = list(user_like)[1]
    for one_kv_pair in kv_pairs:
        key = list(one_kv_pair.items())[0][0]
        movie_list.append(key)
    
    ans_dict = collections.defaultdict(dict)
    n = len(movie_list)
    
    # cartesian
    for i in range(n):
        for j in range(n):
            if i != j:
                ans_dict[str(movie_list[i])][str(movie_list[j])] = 1
    # {"101":{"102": 1, "103":1}, "102":{"103": 1}}
    return ans_dict

def read_exist_matrix(user_like = False, co_occurance = True):
    # read user_like.json
    if user_like:
        with open('pre_stored_matrix/user_like.json') as json_file:
            user_like =json.load(json_file)
            # [{'31': 2.5}, {'2294': 2.0}]
        return user_like
    # read co_occurance.json\
    if co_occurance:
        with open('pre_stored_matrix/co_occurrence.json') as json_file:
            co_occurance_matrix =json.load(json_file)
        
        return co_occurance_matrix

# Item based
def ItemBasedFiltering(user_like, co_occurance_matrix, user_id:str, n=10, file = '/user/user_data/history/ratings.csv', train = False):
    if train == True:
        # step 1 group by user map
        # we call it user-like matrix
        map_res = mapPartition(file, user_2_moive_map)
        # map_res = userid : (movieid : movie rating)
        combine_res = reduce(map_res, combine_func=None, reduce_func= user_2_moive_reduce)
        # combine_res = {1:{101:5,102:3,103:2}, 2:[] ...}

        # save this to our firebase, call it user-like matrix
        
        with open("pre_stored_matrix/user_like.json", "w") as outfile:
            json.dump(dict(combine_res), outfile, indent = 4)
        
        # step 2: for each user's rated movie, do Cartesian Product, then group
        # we call it co-occurance matrix
        co_occurance_matrix = collections.defaultdict(dict)

        for k,v in combine_res.items():
            # 1:{101:5,102:3,103:2} -> {"101":{"102": 1, "103":1}, "102":{"103": 1}}
            this_dict = CartesianProduct_map({k:v})

            # combine
            for k,v in this_dict.items():
                for k2 in v.keys():
                    co_occurance_matrix[k][k2] = co_occurance_matrix[k].get(k2,0) + 1
        co_occurance_matrix = dict(co_occurance_matrix)
        # co-occurance => {"101":{"102":7, "103":8  }...}
        with open("pre_stored_matrix/co_occurance.json", "w") as outfile:
            json.dump(dict(co_occurance_matrix), outfile, indent = 4)
    else:
        # suppose user "1"
        predict_result = collections.defaultdict(int) # movieid:predict_rates

        # user "1" rates 3 for mid 101, then go to co_occurance ["101"]
        # co_occurance ["101"] = {"102" : 38, "103": 75}
        # then predict_result = {"102": 38 * 3, "103": 75 * 3}

        # do the same thing if user "1" rates 2 for mid "303" 
        
        for kv_pair in user_like.items():
            user_watched_movie = kv_pair[0]
            rate = kv_pair[1]

            for related_movies, related_factor in co_occurance_matrix[user_watched_movie].items():
                predict_result[related_movies] += rate * related_factor

                ### debug ouput ###
                print("user have rated movie a: " + user_watched_movie + " " + str(rate) + " point")
                print("movie b (in co-occurence matrix):" + related_movies + " have simlarity(co-occurence time): " + str(related_factor))
                print("So, Predict rating += rate * related_factor = " + str(predict_result[related_movies]))
            
        # {82:2332, 15:1024}
        predict_result = dict(predict_result)

        sorted_res = sorted(predict_result.items(), key = lambda x: x[1], reverse= True)
        top_n = sorted_res[:n]

        print("recommendation ranking: Movie_id: predict rating (the higher the better)")
        print(top_n)
        # output information
        id_list = [int(i[0]) for i in top_n]
        preference_list = [int(i[1]) for i in top_n]

        full_info_res = full_info_checker(id_list)

        order_list = []
        for movie_id in id_list:
            order_list.append(full_info_res[movie_id])
        
        import pandas as pd
        df = pd.DataFrame(order_list)
        df.insert(1, "frequency", preference_list)
        return df


def fast_cat(file):
    import requests
    import json
    url = 'https://dsdi551-8cc6e-default-rtdb.firebaseio.com/'
    dirlist = file.split('/')
    dirlist[0] ='/'
    r = requests.get(url+'metadata/INodeSection/inodes.json')
    res = json.loads(r.text)
    #resdict is a list contains all dict/file name
    
    resdict = dict()
    for k in res.items():
        resdict[list(k)[1]['name']] = list(k)[1]['id']
    r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = resdict[dirlist[-1]]))
    r1 = json.loads(r1.text)
    # --------------------------
    
    # ------ map partition -----
    final_res = []
    for partition in r1:
        lines = json.loads(requests.get(partition[0]).text) 
        
        for single_line in lines:
            final_res.append(single_line)

    return final_res


# filter + rank top N by rates (do not group)
def actor_map(single_line: dict, args) -> dict:
    # args[0] -> movie type ; args[1] -> order by
    if single_line["cast"][0] == "[":
        ans = {}
        this_list = ast.literal_eval(single_line["cast"])    

        for actor in this_list:
            ans[actor] = 1
        return ans
    return None

def actor_combine(map_result, args): # N -> select Top N
    # args[2] -> desc, args[3] -> number of result

    combine_res = []
    for partition in map_result:    # [[{}, {}]]
        # select TOP N
        # utilize heap sort
        import collections
        this_partition_combine_res = collections.defaultdict(int)

        for kv_pair in partition:
            for k in kv_pair.keys():
                this_partition_combine_res[k] += 1

        # this_partition_combine_res = \
        #     dict(sorted(this_partition_combine_res.items(), key = lambda x: x[1], reverse= True)[:N])

        combine_res.append(this_partition_combine_res)    # [[{}, {}], []]

    ### debug ouput ###
    print(list(((dict(combine_res[0])).items()))[0])

    return combine_res

def actor_reduce(combine_res, args):
    # args[2] -> desc, args[3] -> number of result
    import collections
    reduce_res = collections.defaultdict(int)
    for each_partition_combine_res in combine_res:
        for k,v in each_partition_combine_res.items():
            reduce_res[k] += v
    
    sort_res = sorted((dict(reduce_res)).items(), key = lambda x: x[1], reverse= True)
    sort_res = sort_res[:10]

    ### debug ouput ###
    print(sort_res[0])

    return sort_res

# filter_rank
def actor_rank(movie_type = "Action", order_by = "Rating", N = 10, file = '/movie/movie_info.csv', Desc = True):
    # args = [movie_type, order_by, Desc, N]
    map_res = mapPartition(file, actor_map, args=None)
    sort_res = reduce(map_res, actor_combine, actor_reduce, args=None)
    
    import pandas as pd
    header = ["Actor", "Popularity"]
    df = pd.DataFrame(sort_res, columns = header)
    df.index += 1
        
    return df

# filter + rank top N by rates (do not group)
def director_map(single_line: dict, args) -> dict:
    # args[0] -> movie type ; args[1] -> order by
    if single_line["director"][0] != "U":
        ans = {}
        ans[single_line["director"]] = 1
        return ans
    return None

def director_combine(map_result, args): # N -> select Top N
    # args[2] -> desc, args[3] -> number of result
    ### debug ouput ###
    print_single_reult = False

    combine_res = []
    for partition in map_result:    # [[{}, {}]]
        # select TOP N
        # utilize heap sort
        import collections
        this_partition_combine_res = collections.defaultdict(int)

        for kv_pair in partition:
            for k in kv_pair.keys():
                this_partition_combine_res[k] += 1
        
                ### debug ouput ###
                if print_single_reult == False:
                    print_single_reult = True
                    print(this_partition_combine_res)

        combine_res.append(this_partition_combine_res)    # [[{}, {}], []]

    ### debug ouput ###
    print(list(((dict(combine_res[0])).items()))[0])

    return combine_res

def director_reduce(combine_res, args):
    # args[2] -> desc, args[3] -> number of result
    import collections
    reduce_res = collections.defaultdict(int)
    for each_partition_combine_res in combine_res:
        for k,v in each_partition_combine_res.items():
            reduce_res[k] += v

    sort_res = sorted((dict(reduce_res)).items(), key = lambda x: x[1], reverse= True)
    sort_res = sort_res[:10]

    ### debug ouput ###
    print(sort_res[0])

    return sort_res

# filter_rank
def director_rank(movie_type = "Action", order_by = "Rating", N = 10, file = '/movie/movie_info.csv', Desc = True):
    # args = [movie_type, order_by, Desc, N]
    map_res = mapPartition(file, director_map, args=None)
    sort_res = reduce(map_res, director_combine, director_reduce, args=None)
    
    import pandas as pd
    header = ["Director", "Popularity"]
    df = pd.DataFrame(sort_res, columns = header)
    df.index += 1
        
    return df