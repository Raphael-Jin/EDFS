def mapPartition(file, map_func, args = None):
    map_res = []    
    # --------- get partition ----------
    import requests
    import json
    url = 'https://dsdi551-8cc6e-default-rtdb.firebaseio.com/'
    dirlist = file.split('/')
    dirlist[0] ='/'
    r = requests.get(url+'metadata/INodeSection/inodes.json')
    res = json.loads(r.text)
    # resdict is a list contains all dict/file name
    # print(file)
    resdict = dict()
    for k in res.items():
        resdict[list(k)[1]['name']] = list(k)[1]['id']
    r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = resdict[dirlist[-1]]))
    r1 = json.loads(r1.text)
    # --------------------------

    # ------ map partition -----
    print_single_reult = False

    for partition in r1:
        this_partition_res = []
        
        lines = json.loads(requests.get(partition[0]).text) 
        # print(partition[0], len(lines)) # debug
        for single_line in lines:
            k_v_pair = map_func(single_line, args) # args[0] -> movie type ; args[1] -> order by
            if k_v_pair is not None:
                this_partition_res.append(k_v_pair)

                if print_single_reult == False:
                    print("\n" + map_func.__name__)
                    print("Map begin:")
                    print("Map Result")
                    print_single_reult = True
                    print(k_v_pair)

        map_res.append(this_partition_res)  
    return map_res

def reduce(map_res, combine_func = None, reduce_func = None, args = None):
    if combine_func:
        print("\n" + combine_func.__name__)
        print("Combine begin:")
        combine_res = combine_func(map_res, args) # args[2] -> desc, args[3] -> number of result

        print("\n" + reduce_func.__name__)
        print("Reduce begin")
        # map_res => [[k:v,k:v],...., [k:v]]
        print("Reduce result:") 
        reduce_res = reduce_func(combine_res, args) # args[2] -> desc, args[3] -> number of result
    else:
        print("\n" + reduce_func.__name__)
        print("Reduce begin")
        # map_res => [[k:v,k:v],...., [k:v]]
        print("Reduce result:")
        reduce_res = reduce_func(map_res)
    
    return reduce_res