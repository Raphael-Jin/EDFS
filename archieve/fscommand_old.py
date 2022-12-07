"""
@title: command.py
@author: susie
"""

import requests
import json
from datetime import datetime
import pandas as pd
import os
import math
# jq: 'https://movie551-default-rtdb.firebaseio.com/'
url = 'https://dsdi551-8cc6e-default-rtdb.firebaseio.com/'
# url = 'https://movie551-default-rtdb.firebaseio.com/'
    
def start():
    requests.put(url+'.json',json = {})
    dn = int(input('How many datanodes?(0-100): '))
    size = int(input('What\'s storage size of each datanode(GB): '))
    size = 1000*size # convert into MB
    rp = int(input('How many replications?(less than datanodes): '))
    if (rp>=1 and rp<=dn and dn>=1 and dn<=100):
        requests.patch(url+'metadata/INodeSection.json',json = {'numInodes':0,'lastInodeId':'','replication':rp,'numdatanodes':dn,'datanodesize(MB)':size})
        requests.put(url+'data.json',json = '')
        for x in range(1,dn+1):
            requests.put(url+'data/datanodes{id}.json'.format(id = x),json = '')
            requests.patch(url+'data/datanodes{id}.json'.format(id = x),json = {"usage(MB)":0})
    else:
        print('Fail:input is out of range')
        return
    requests.put(url+'metadata/INodeSection/inodes.json',json = '')
    requests.put(url+'metadata/INodeDirectorySection.json',json = '')
    mkdir('/')


def  mkdir(newdir,user = 'user', group = 'group',force_allowed=False):
    if not(newdir == '/'):
        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)
        
        dirlist = newdir.split('/')
        dirlist[0] ='/'
        ########## PERMISSION ############################
        if force_allowed == False:
            ipath = newdir.rsplit('/',1)[0]
            if ipath == '':
                ipath = '/'
            perm = permission(ipath=ipath, user = user, group = group)
            if perm == 'everyone':
                print('Fail:no permission')
                return
            elif perm == 'group':
                print('Fail:no permission')
                return
        ##################################################
        #check whether directory has exited
        pdir = newdir.rsplit('/',1)[0]
        if pdir == '':
            pdir = '/'
        pdirlist = pdir.split('/')
        pdirlist[0] ='/'
        while ('' in pdirlist):
            pdirlist.remove('')
        
        parentid = check_parent_child(res3, pdirlist)
        if parentid == 'fail':
            return('Fail:parent directory is incorrect')
        
        brother = ls(pdir)
        if str(dirlist[-1]) in brother:
            flag = input('Directory \'{dirname}\' exists, Continue to overwrite?(y/n): '.format(dirname = newdir)).lower()
            if (flag == 'n'):
                print('Fail:mkdir is interrupted by user')
                return
            else:
                tdi = check_parent_child(res3, newdir)
                try: 
                    res3[tdi]
                    print('Fail:non-empty directory cannot be overwritten')
                    return
                except:
                    td = dirlist[-1] #target to delete   
    else:
        dirlist =list('/')
    
    r1 = requests.get(url+'metadata/INodeSection/lastInodeId.json')
    res1 = json.loads(r1.text)
    
    if (len(str(res1)) == 0):
        inum = 1000
    else:
        inum = res1 + 1  
    try:
        requests.delete(url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(td)))
        requests.delete(url + 'metadata/INodeDirectorySection/{p}/{i}.json'.format(p = parentid ,i = str(tdi)))
        r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
        res2 = json.loads(r2.text)
        requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2-1})
        #lastnodeid doesn't need to change since new directory will be updated soon
    except:
        pass
    finally:
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
        if group == 'admin':
            pmson = '775'
        elif group == 'group':
            pmson = '777'
        else:
            pmson = '755'
        a = requests.patch(url_node,json = 
                           {'id':inum,
                            'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                            'name':dirlist[-1],
                            # default user->group, permission as 755
                            'owner':user,
                            'group':group,
                            'permission':pmson,
                            'type':'directory'})
        print(a)
        if (newdir !='/'):
            try:
                dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i= parentid))
                dres = json.loads(dr.text)
                if (dres == None):
                    dres = []
            except:
                dres = []
            if (len(dres) == 0 or ()): # first child
                b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=parentid), json = {'0':inum})
            else:
                b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=parentid), json = {len(dres):inum})
            print(b)
        requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
        r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
        res2 = json.loads(r2.text)
        requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})

def check_parent_child(res3,plist,pid = 1000):
    if len(plist) == 1:
        return pid
    plist.pop(0)
    pidlist_temp = res3[str(pid)]
    for k in pidlist_temp:
        pname_temp = requests.get(url + 'metadata/INodeSection/inodes/{i}/name.json'.format(i = k))
        pname_temp = json.loads(pname_temp.text)
        if pname_temp == plist[0]:
            pid = k
            break
        if k == pidlist_temp[len(pidlist_temp)-1]:
            return('fail')
    return check_parent_child(res3,plist,pid)
    

def ls(dir,user='user',group='group'):
    ########## PERMISSION ############################
    ipath = dir
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        print('Fail:no permission')
        return
    ##################################################

    #res3 is relationship between inodes
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)

    dirlist = dir.split('/')
    dirlist[0] ='/'
    while ('' in dirlist):
        dirlist.remove('')
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    if pid == 'fail':
        return('Fail:parent directory is incorrect')
    else:
        clist = []
        try:
            cidlist = res3[str(pid)]
            for c in cidlist:
                cname_temp = requests.get(url + 'metadata/INodeSection/inodes/{i}/name.json'.format(i = c))
                cname_temp = json.loads(cname_temp.text)
                clist.append(cname_temp)
        except:
            pass
        finally:
            return clist

def put(fileloc,dir,p,storecal=0,storeway="hashing",user ='user',group = 'group'):
    ########## PERMISSION ############################
    ipath = dir
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        print('Fail:no permission')
        return
    elif perm == 'group':
        print('Fail:no permission')
        return
    ##################################################
    #check p should be less than maximum of datanodes
    rpp = requests.get(url+'metadata/INodeSection/numdatanodes.json')
    respp = json.loads(rpp.text)
    if (p>respp):
        print('Fail:#partition is out of  range')
        return

    #res3 is relationship between inodes
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)

    dirlist = dir.split('/')
    dirlist[0] ='/'
    while ('' in dirlist):
        dirlist.remove('')
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    if pid == 'fail':
        return('Fail:parent directory is incorrect')
        
    #check dir is a directory not a file
    rd = requests.get(url+'metadata/INodeSection/inodes/{dir_id}/type.json'.format(dir_id = pid))
    rdes = json.loads(rd.text)
    if (rdes != 'directory'):
        print('Fail:non-directory cannot be put a file')
        return
    
    #check fileloc exists
    if (fileloc[-1] =='v'):
        try:
            df = pd.read_csv(fileloc)
        except:
            print('Fail:file does not exist')
            return
    else:
        try:
            df = pd.read_json(fileloc,orient='index')      
        except:
            print('Fail:file does not exist')
            return
    try:
        df = df.sort_values(df.columns[storecal])
    except:
        pass
    #check whether file has exited
    brother = ls(dir)
    filename = fileloc.split('/')[-1]
    if (filename in brother):
        flag = input('File \'{filename}\' exits, Continue to overwrite?(y/n): '.format(filename = filename).lower())
        if (flag == 'n'):
            print('Fail:put is interrupted by user')
            return
        else:
            b = dir + filename
            blist = b.split('/')
            blist[0]='/'
            tdi = check_parent_child(res3, blist) #target to delete
            td = filename   


    #check whether put the file will be out of the range of datanode
    fsize = os.stat(fileloc).st_size #Byte
    fsize = fsize / (1000*1000) #MB
    
    dinfo = requests.get(url+'metadata/INodeSection.json')
    dinfo = json.loads(dinfo.text)
    
    rr = dinfo['replication']
    rdn = dinfo ['numdatanodes']
    dsize = dinfo['datanodesize(MB)']
    
    psize = fsize / p
    
    minsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToLast=1')
    minsize = json.loads(minsize.text)
    for x in minsize.values():
        minsize = dsize - x['usage(MB)']
    if minsize < psize * (math.floor(p * rr /rdn)):
        print('Fail: no space to store the file1')
        return
    
    mr = (p * rr) %rdn
    maxsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToFirst={maxnodenum}'.format(maxnodenum = mr))
    maxsize = json.loads(maxsize.text)
    for x in maxsize.values():
        if (dsize - x['usage(MB)']) < psize * (math.ceil(p * rr /rdn)):
            print('Fail: no space to store the file2')
            return

    
    res1 = dinfo['lastInodeId']
    inum = res1 + 1  
    
    try:
        requests.delete(url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(td)))
        requests.delete(url + 'metadata/INodeDirectorySection/{p}/{i}.json'.format(p = pid ,i = str(tdi)))
        r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
        res2 = json.loads(r2.text)
        requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2-1})
        #lastnodeid doesn't need to change since new directory will be updated soon
    except:
        pass
    finally:
        #setup metadata
        if group == 'admin':
            pmson = '775'
        elif group == 'group':
            pmson = '777'
        else:
            pmson = '755'
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
        a = requests.patch(url_node,json = 
                           {'id':inum,
                            'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                            'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                            'name':filename,
                            'partition':p,
                            # default user->group, permission as 755
                            'owner':user,
                            'group':group,
                            'permission':pmson,
                            'storeway':storeway,
                            'storecal':storecal,
                            'type':'file',
                            'file_size':fsize, # not include replication size
                            'partition_size':psize,
                            'row_size':fsize/len(df)})
        print(a)
        requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
        r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
        res2 = json.loads(r2.text)
        requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})
        
       #setup datanodes
        
        urllist = []
        for v in res3.values():
            if type(v[0]) == list:
                for x in range(0,len(v)):
                    for z in range(0,len(v[x])):
                        urllist.append(v[x][z])
        
        udf = pd.DataFrame(columns=['datanode','partition'])
        for ui in range(0,len(urllist)):
            u = urllist[ui]
            u = u.rsplit('/',2)
            u[2] = u[2].split('.')[0]
            u.pop(0)
            udf.loc[ui]=u
            
        mdf = pd.DataFrame(udf.groupby(['datanode'],as_index=False).agg({'partition':[max,len]}))
        mdf = mdf.sort_values('datanode',ascending = 1)
        
        mdf_left = pd.DataFrame(['datanodes{inum}'.format(inum=i) for i in range(1,rdn+1)],columns=['datanode'])
        mdf = pd.merge(mdf_left,mdf,how='left',left_on='datanode',right_on='datanode')
        mdf = mdf.fillna(0)
        mdf.columns = ['datanodes','len','max']
        
        
        danamelist = list(mdf['datanodes'])
        dalenlist = list(mdf['max'])
        daidlist = []
        for x in range(0,len(mdf)):
            if int(mdf['max'][x]) == 0 & int(mdf['len'][x]) == 0:
                daidlist.append(0)
            else:
                daidlist.append(int(mdf['max'][x]))
        

        #setup datanodes
        dasizelist = []
        for s in range(1,rdn+1):
            dasizelist.append(dsize-float(requests.get(url+'data/datanodes{nums}/usage(MB).json'.format(nums = s)).text))
            
        dalen = pd.DataFrame({'datanodes':danamelist,'length':dalenlist,'lastid':daidlist,'size':dasizelist})
        dalen = dalen.sort_values('size',ascending = 0)
       
        plist = list(range(0,p))
        for i in range(0,rr-1):
            plist = plist + plist
        plist.sort()
            
        plocdict = dict()
        if (storeway == 'hashing'):
            i = 0
            for x in plist:
                df_temp = df[x:len(df):p]
                js_temp = df_temp.to_dict(orient='records')
                b = requests.put(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]),
                                json = js_temp)
                
                plocdict.setdefault(x,[]).append(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]))
                dalen.iloc[i,2] = int(dalen.iloc[i,2])+1
                dalen.iloc[i,1] = int(dalen.iloc[i,1])+1
                dalen.iloc[i,3] = int(dalen.iloc[i,3]) - psize
                i =  i + 1
                if (i == len(dalen)):
                    i = 0
        for x in range(0,len(dalen)):
            requests.put(url + 'data/{dnode}/usage(MB).json'.format(dnode = dalen.iloc[x,0]), json = dsize - dalen.iloc[x,3])
        #get partition location in data
        requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=inum), json = plocdict)
        
        try:
            dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i=pid))
            dres = json.loads(dr.text)
            if (dres == None):
                dres = []
        except:
            dres = []
        if (len(dres) == 0 or ()): # first child
            b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {'0':inum})
        else:
            b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {len(dres):inum})
        print(b)
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = pid)
        a = requests.patch(url_node,json = 
                          {'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})


def rm(file,user='user',group='group'):
    ########## PERMISSION ############################
    ipath = file.rsplit('/',1)[0]
    if ipath == '':
        ipath = '/'
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        return ('Fail:no permission')
        
    elif perm == 'group':
        return ('Fail:no permission')    
    ##################################################
    try:
        a= ls(file)
    except:
        return ('Fail:cannot find the file/directory to remove1')
    finally:
        if (a[0:4] == 'Fail'):
            return ('Fail:cannot find the file/directory to remove2')
            
        r = requests.get(url+'metadata/INodeSection/inodes.json')
        res = json.loads(r.text)

        #res3 is relationship between inodes
        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)

        dirlist = file.split('/')
        dirlist[0] ='/'
        dirlist1 = dirlist
        pid = check_parent_child(res3, dirlist1)
        
        rtype = res[str(pid)]['type']
        
        if (rtype == 'directory'):
        #directory:
        #1. is directory empty?
        #2.remove inode 
        #3. remove inode directory ane its parent directory (optional)
            try:
                res3[str(pid)]
                return ('Fail:non-empty directory cannot be removed')
            except:
                pass
        else:
        #file:
        #1. remove data based on indedirectory 
        #2. remove inode directory (both itself and its parent if relationship isempty) 
        #3.remove inode-change info.
            psize = requests.get(url+ 'metadata/INodeSection/inodes/{inodeid}/partition_size.json'.format(inodeid = pid))
            psize = json.loads(psize.text)
            for x in res3[str(pid)]:
                for y in x:
                    requests.delete(y)
                    usage_temp = requests.get(y.rsplit('/',1)[0] + '/usage(MB).json')
                    requests.put(y.rsplit('/',1)[0] + '/usage(MB).json',json = float(usage_temp.text) - psize )
        
        requests.delete(url+ 'metadata/INodeSection/inodes/{inodeid}.json'.format(inodeid = pid))
        r = requests.get(url+'metadata/INodeSection/inodes.json')
        res = json.loads(r.text)
        requests.put(url + 'metadata/INodeSection/lastInodeId.json',json = int(max(res.keys())))
        requests.put(url+'metadata/INodeSection/numInodes.json',json = len(res))
        
        requests.delete(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
        for x in res3.keys():
            if (pid in res3[x]):
                rdp = requests.get(url+'metadata/INodeDirectorySection/{pnodeid}.json'.format(pnodeid = int(x)))
                rdp = json.loads(rdp.text)
                requests.delete(url+ 'metadata/INodeDirectorySection/{pnodeid}/{inodeid}.json'.format(pnodeid = int(x),inodeid = rdp.index(pid)))
                rplist = requests.get(url + 'metadata/INodeDirectorySection/{pnodeid}.json'.format(pnodeid = x))
                rplist = json.loads(rplist.text)
                if (rplist == []):
                    requests.delete(url+ 'metadata/INodeDirectorySection/{pnodeid}.json'.format(pnodeid = x))
        
        dirlist2 = file.rsplit('/',1)[0]
        dirlist2 = dirlist2.split('/')
        dirlist2[0] ='/'

        parentid = check_parent_child(res3, dirlist2)
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = parentid)
        a = requests.patch(url_node,json = 
                          {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
        return ('Success:Remove the file/directory successfully')
        

def cat(file,user='user',group='group',force_convert_to_json=False):
    ######### PERMISSION ############################
    ipath = file.rsplit('/',1)[0]
    if ipath == '':
        ipath = '/'
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        return('Fail:no permission')
        
    ##################################################
    #1.confirm ls(file)
    try:
        a= ls(file)
    except:
        return('Fail:cannot find the file/directory to cat1')
    finally:
        if (a[0:4] == 'Fail'):
            return(a)
    #2.get loc. of data in datanodes
        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)
        
        dirlist = file.split('/')
        dirlist[0] ='/'
        dirlist1 = dirlist
        pid = check_parent_child(res3, dirlist1)
            
        r = requests.get(url+'metadata/INodeSection/inodes.json')
        res = json.loads(r.text)
        
        r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
        r1 = json.loads(r1.text)
        
    #3.get data: name sortindex like p1:0,3,6(by partition get in namenode) and archive
        dflist = list()
        p = res[str(pid)]['partition']
        flag = ''
        for x in range(0,len(r1)):
            rtemp = requests.get(r1[x][0])
            rtemp = json.loads(rtemp.text)
            if type(rtemp) == str:
                flag = 'stop'
                break
            else:
                df1 = pd.DataFrame(rtemp,index = list(range(x,x+len(rtemp)*p,p)))
                dflist.append(df1)
        if flag != 'stop':
            df = pd.concat(dflist,ignore_index=False).sort_index()
        else:
            df = []
        
    #4.1 csv: print dataframe
    #4.2 json: print json
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = res[str(pid)]['id'])
        a = requests.patch(url_node,json = 
                          {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
        if force_convert_to_json == False:
            return df
        else:
            st = df.to_dict(orient='records')
            rst = {i : st[i] for i in range(len(st))}
            return rst     

def getpartition(file,user='user',group='group'):
    ########## PERMISSION ############################
    ipath = file.rsplit('/',1)[0]
    if ipath == '':
        ipath = '/'
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        return('Fail:no permission')
    ##################################################
    try:
        a= ls(file)
    except:
        return ('Fail:cannot find the file/directory to getpartition1')
    finally:
        if (a[0:4] == 'Fail'):
            return ('Fail:cannot find the file/directory to getpartition2')
        
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)
    dirlist = file.split('/')
    dirlist[0] ='/'
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    
    r = requests.get(url+'metadata/INodeSection/inodes.json')
    res = json.loads(r.text)

    r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
    r1 = json.loads(r1.text)
    
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = r1[0])
    a = requests.patch(url_node,json = 
                      {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
    
    if (res[str(pid)]['type'] == 'file'):
        loc = dict()
        for x in range(0,len(r1)):
            loc[x]=r1[x]  
        return (loc)
    else:
        return ('Fail:input is not a file')
        

def readpartition(file,partitionid,user='user',group='group',force_convert_to_json=False):
    ########## PERMISSION ############################
    ipath = file.rsplit('/',1)[0]
    if ipath == '':
        ipath = '/'
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        return ('Fail:no permission')
    ##################################################
    try:
        a= ls(file)
    except:
        return ('Fail:cannot find the file/directory to readpartition1')
    finally:
        if (a[0:4] == 'Fail'):
            return ('Fail:cannot find the file/directory to readpartition2')

        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)
        
        dirlist = file.split('/')
        dirlist[0] ='/'
        dirlist1 = dirlist
        
        pid = check_parent_child(res3, dirlist1)

        r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
        r1 = json.loads(r1.text)
        
        df = requests.get(r1[partitionid][0])
        df = json.loads(df.text)
        if type(df) == str:
            df= []
        else:
            df = pd.DataFrame(df)
        
        url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = r1[0])
        a = requests.patch(url_node,json = 
                          {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
        
        if force_convert_to_json == False:
            return (df)
        else:
            st = df.to_dict(orient='records')
            rst = {i : st[i] for i in range(len(st))}
            return rst 
        

def readpartition_for_add(file,partitionid):
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)
    
    dirlist = file.split('/')
    dirlist[0] ='/'
    dirlist1 = dirlist
    
    pid = check_parent_child(res3, dirlist1)

    r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
    r1 = json.loads(r1.text)
    
    df = requests.get(r1[partitionid][0])
    df = json.loads(df.text)
    if type(df) != list:
        df=[]
    else:
        df = pd.DataFrame(df)
    return df

        
def addpartition(file, dict_add): #dict_add is in the format of dict
    if (type(dict_add) != dict):
        print('Fail: added record is not in the format of dict')
        return

    try:
        dict_pat = getpartition(file)
    except:
        print('Fail: cannot getpartititon of file1')
        return
    try:
        if (dict_pat[0:4] == 'Fail'):
            print('Fail: cannot getpartititon of file2')
            return
    except:
        pass
    # readpartition
    # comfirm total rows of data: N -> the first data is N+1
    size =0
    size_p = dict()
    for x in range(0,len(dict_pat)):
        temp_s = len(readpartition_for_add(file,x))
        size_p[x] = temp_s
        size = size + temp_s
    
    put_p = (size+1) % len(dict_pat)
    r = requests.get(url + 'metadata/INodeSection/inodes.json?orderBy="name"&equalTo="{nodename}"'.format(nodename = file.split('/')[-1]))
    r = json.loads(r.text)
    for x in r.keys():
        rsize = r[x]['row_size']
        fsize = r[x]['file_size']
        nodeid = str(r[x]['id'])
    
    for url_x in dict_pat[put_p]:
        requests.put(url_x.replace('.json','/') + '{lastid}.json'.format(lastid = size_p[put_p]),json = dict_add)
        usage_temp = requests.get(url_x.rsplit('/',1)[0] + '/usage(MB).json')
        requests.put(url_x.rsplit('/',1)[0] + '/usage(MB).json',json = float(usage_temp.text) + rsize )
   
    
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = nodeid)
    requests.patch(url_node,json = 
                   {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                    'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                    'file_size':fsize + rsize})
    


def permission(ipath, user = 'user', group = 'group'):
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)
    
    if ipath == '/':
        inum = 1000
    else:
        dirlist = ipath.split('/')
        dirlist[0] ='/'
        dirlist1 = dirlist
        inum = check_parent_child(res3, dirlist1)
       
    ri = requests.get(url + 'metadata/INodeSection/inodes.json?orderBy="$key"&equalTo="{nodeid}"'.format(nodeid = inum))
    ri = json.loads(ri.text)
    
    ig = ri[str(inum)]['group']
    io = ri[str(inum)]['owner']
    
    if (ig == 'group') & (io =='user'): #public
        return ('owner')
    elif (user == 'user') & (group == 'group'):
        return ('owner')
    elif (ig == 'admin') & (group == 'admin'):
        return ('owner')
    elif (io == user):
        return ('owner')
    elif (ig == group):
        return ('group')
    else:
        return ('everyone')

          
def browse(target,user='user',group='group'):
    path = target
    path = path.split('/')
    path[0] = '/'
    
    for x in path:
        if x == '':
            path.pop(path.index(x))
    
    ri = requests.get(url + 'metadata/INodeSection/inodes.json?orderBy="name"&equalTo="{nodename}"'.format(nodename = path[-1]))
    jri = json.loads(ri.text)
    nodeid = str(jri).split("'")[1]
    rt = jri[nodeid]['type']
            
    if rt != 'file':
        rd = requests.get(url+'metadata/INodeDirectorySection.json')
        rd = json.loads(rd.text)
        try:
            child = rd[nodeid]
        except:
            df = pd.DataFrame()
            return rt, df
        
        r = requests.get(url+'metadata/INodeSection/inodes.json')
        r = json.loads(r.text)
        
        df = list()
        for c in child:
            c = str(c)
            if 'None' in c:
                pass
            else:
                df.append(r[c])
        df = pd.DataFrame(df)
        for x in range(0,len(df)):
            df['mtime'][x] = datetime.utcfromtimestamp(df['mtime'][x]/1000).strftime("%m/%d/%Y %H:%M")
        if group != "admin":
            df = df[(df["group"]==group)|(df["group"]=="group")]    
        if (target == '/'):
            df['goto'] = target + df['name']
        else:
            df['goto'] = target + '/'+ df['name']
        return rt, df
    elif rt == 'file':
        df = pd.DataFrame([jri[nodeid]])
        for x in range(0,len(df)):
            df['mtime'][x] = datetime.utcfromtimestamp(df['mtime'][x]/1000).strftime("%m/%d/%Y %H:%M")
            df['atime'][x] = datetime.utcfromtimestamp(df['atime'][x]/1000).strftime("%m/%d/%Y %H:%M")
        return rt, df
    
def put_st(fileloc,name,fsize,dir,p,storecal=0,storeway="hashing",user ='user',group = 'group'):
    ########## PERMISSION ############################
    ipath = dir
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        return('Fail:no permission')
    elif perm == 'group':
        return('Fail:no permission')
    ##################################################
    #check p should be less than maximum of datanodes
    rpp = requests.get(url+'metadata/INodeSection/numdatanodes.json')
    respp = json.loads(rpp.text)
    if (p>respp):
        print('Fail:#partition is out of  range')
        return

    #res3 is relationship between inodes
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)

    dirlist = dir.split('/')
    dirlist[0] ='/'
    while ('' in dirlist):
        dirlist.remove('')
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    if pid == 'fail':
        return('Fail:parent directory is incorrect')
        
    #check dir is a directory not a file
    rd = requests.get(url+'metadata/INodeSection/inodes/{dir_id}/type.json'.format(dir_id = pid))
    rdes = json.loads(rd.text)
    if (rdes != 'directory'):
        return('Fail:non-directory cannot be put a file')
    
    #check fileloc exists
    if (name[-1] =='v'):
        try:
            df = pd.read_csv(fileloc)
        except:
            return('Fail:file does not exist')
    else:
        try:
            df = pd.read_json(fileloc,orient='index')      
        except:
            return('Fail:file does not exist')
    
    df = df.sort_values(df.columns[storecal])
    
    #check whether file has exited
    brother = ls(dir)
    if name in brother:
        return('Fail:file with the same name exists, pls remove first')

    #check whether put the file will be out of the range of datanode
    fsize = fsize / (1000*1000) #MB
    
    dinfo = requests.get(url+'metadata/INodeSection.json')
    dinfo = json.loads(dinfo.text)
    
    rr = dinfo['replication']
    rdn = dinfo ['numdatanodes']
    dsize = dinfo['datanodesize(MB)']
    
    psize = fsize / p
    
    minsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToLast=1')
    minsize = json.loads(minsize.text)
    for x in minsize.values():
        minsize = dsize - x['usage(MB)']
    if minsize < psize * (math.floor(p * rr /rdn)):
        return('Fail: no space to store the file1')
    
    mr = (p * rr) %rdn
    maxsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToFirst={maxnodenum}'.format(maxnodenum = mr))
    maxsize = json.loads(maxsize.text)
    for x in maxsize.values():
        if (dsize - x['usage(MB)']) < psize * (math.ceil(p * rr /rdn)):
            return('Fail: no space to store the file2')

    res1 = dinfo['lastInodeId']
    inum = res1 + 1  
    
    #setup metadata
    if group == 'admin':
        pmson = '775'
    elif group == 'group':
        pmson = '777'
    else:
        pmson = '755'
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
    requests.patch(url_node,json = 
                       {'id':inum,
                        'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'name':name,
                        'partition':p,
                        # default user->group, permission as 755
                        'owner':user,
                        'group':group,
                        'permission':pmson,
                        'storeway':storeway,
                        'storecal':storecal,
                        'type':'file',
                        'file_size':fsize, # not include replication size
                        'partition_size':psize,
                        'row_size':fsize/len(df)})
    requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
    r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
    res2 = json.loads(r2.text)
    requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})
        
    #setup datanodes
     
    urllist = []
    for v in res3.values():
        if type(v[0]) == list:
            for x in range(0,len(v)):
                for z in range(0,len(v[x])):
                    urllist.append(v[x][z])
    
    udf = pd.DataFrame(columns=['datanode','partition'])
    for ui in range(0,len(urllist)):
        u = urllist[ui]
        u = u.rsplit('/',2)
        u[2] = u[2].split('.')[0]
        u.pop(0)
        udf.loc[ui]=u
        
    mdf = pd.DataFrame(udf.groupby(['datanode'],as_index=False).agg({'partition':[max,len]}))
    mdf = mdf.sort_values('datanode',ascending = 1)
    danamelist = list(mdf['datanode'])
    dalenlist = list(mdf['partition']['len'])
    daidlist = [int(x)+1 for x in mdf['partition']['max']]
    
    #setup datanodes
    dasizelist = []
    for s in range(1,rdn+1):
        dasizelist.append(dsize-float(requests.get(url+'data/datanodes{nums}/usage(MB).json'.format(nums = s)).text))
        
    dalen = pd.DataFrame({'datanodes':danamelist,'length':dalenlist,'lastid':daidlist,'size':dasizelist})
    dalen = dalen.sort_values('size',ascending = 0)
   
    plist = list(range(0,p))
    for i in range(0,rr-1):
        plist = plist + plist
    plist.sort()
        
    plocdict = dict()
    if (storeway == 'hashing'):
        i = 0
        for x in plist:
            df_temp = df[x:len(df):p]
            js_temp = df_temp.to_dict(orient='records')
            requests.put(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]),
                            json = js_temp)
            
            plocdict.setdefault(x,[]).append(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]))
            dalen.iloc[i,2] = dalen.iloc[i,2]+1
            dalen.iloc[i,1] = dalen.iloc[i,1]+1
            dalen.iloc[i,3] = dalen.iloc[i,3] - psize
            i =  i + 1
            if (i == len(dalen)):
                i = 0
    for x in range(0,len(dalen)):
        requests.put(url + 'data/{dnode}/usage(MB).json'.format(dnode = dalen.iloc[x,0]), json = dsize - dalen.iloc[x,3])
    #get partition location in data
    requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=inum), json = plocdict)
    
    try:
        dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i=pid))
        dres = json.loads(dr.text)
        if (dres == None):
            dres = []
    except:
        dres = []
    if (len(dres) == 0 or ()): # first child
        requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {'0':inum})
    else:
        requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {len(dres):inum})
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = pid)
    requests.patch(url_node,json = 
                      {'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
    return ('Success:Upload a file successfully~')



def  mkdir_st(newdir,user = 'user', group = 'group'):
    if not(newdir == '/'):
        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)
        
        dirlist = newdir.split('/')
        dirlist[0] ='/'
        ########## PERMISSION ############################
        ipath = newdir.rsplit('/',1)[0]
        if ipath == '':
            ipath = '/'
        perm = permission(ipath=ipath, user = user, group = group)
        if perm == 'everyone':
            return ('Fail:no permission')
         
        elif perm == 'group':
            return ('Fail:no permission')
        
        ##################################################
        pdir = newdir.rsplit('/',1)[0]
        if pdir == '':
            pdir = '/'
        pdirlist = pdir.split('/')
        pdirlist[0] ='/'
        while ('' in pdirlist):
            pdirlist.remove('')
        
        parentid = check_parent_child(res3, pdirlist)
        if parentid == 'fail':
            return('Fail:parent directory is incorrect')
        
        brother = ls(pdir)
        if str(dirlist[-1]) in brother:
            return('Fail: directory with the same name exists, pls remove first')
    else:
        dirlist =list('/')
    
    r1 = requests.get(url+'metadata/INodeSection/lastInodeId.json')
    res1 = json.loads(r1.text)
    
    if (len(str(res1)) == 0):
        inum = 1000
    else:
        inum = res1 + 1  
    
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
    if group == 'admin':
        pmson = '775'
    elif group == 'group':
        pmson = '777'
    else:
        pmson = '755'
    a = requests.patch(url_node,json = 
                       {'id':inum,
                        'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'name':dirlist[-1],
                        # default user->group, permission as 755
                        'owner':user,
                        'group':group,
                        'permission':pmson,
                        'type':'directory'})
    print(a)
    if (newdir !='/'):
        try:
            dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i=parentid))
            dres = json.loads(dr.text)
            if (dres == None):
                dres = []
        except:
            dres = []
        if (len(dres) == 0 or ()): # first child
            b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=parentid), json = {'0':inum})
        else:
            b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=parentid), json = {len(dres):inum})
        print(b)
    requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
    r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
    res2 = json.loads(r2.text)
    requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})
    return ('Success:Make a new directory successfully')

def put_efile(name,dir,fsize=0,p=1,storecal=0,storeway="hashing",user ='user',group = 'group'):
    #check whether put the file will be out of the range of datanode
    dinfo = requests.get(url+'metadata/INodeSection.json')
    dinfo = json.loads(dinfo.text)
    
    rr = dinfo['replication']
    rdn = dinfo ['numdatanodes']
    dsize = dinfo['datanodesize(MB)']
    
    psize = fsize / p
    
    res1 = dinfo['lastInodeId']
    inum = res1 + 1  
        
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
    requests.patch(url_node,json = 
                       {'id':inum,
                        'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'name':name,
                        'partition':p,
                        # default user->group, permission as 755
                        'owner':user,
                        'group':group,
                        'permission':'770',
                        'storeway':storeway,
                        'storecal':storecal,
                        'type':'file',
                        'file_size':fsize, # not include replication size
                        'partition_size':fsize,
                        'row_size':60/(1000*1000)}) #60KB->MB
    requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
    
    res2 = dinfo['numInodes']
    requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})
    
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)
    
    dirlist = dir.split('/')
    dirlist[0] ='/'
    while ('' in dirlist):
        dirlist.remove('')
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    
    urllist = []
    for v in res3.values():
        if type(v[0]) == list:
            for x in range(0,len(v)):
                for z in range(0,len(v[x])):
                    urllist.append(v[x][z])
    
    udf = pd.DataFrame(columns=['datanode','partition'])
    for ui in range(0,len(urllist)):
        u = urllist[ui]
        u = u.rsplit('/',2)
        u[2] = u[2].split('.')[0]
        u.pop(0)
        udf.loc[ui]=u
        
    mdf = pd.DataFrame(udf.groupby(['datanode'],as_index=False).agg({'partition':[max,len]}))
    mdf = mdf.sort_values('datanode',ascending = 1)
    danamelist = list(mdf['datanode'])
    dalenlist = list(mdf['partition']['len'])
    daidlist = [int(x)+1 for x in mdf['partition']['max']]
    
    #setup datanodes
    dasizelist = []
    for s in range(1,rdn+1):
        dasizelist.append(dsize-float(requests.get(url+'data/datanodes{nums}/usage(MB).json'.format(nums = s)).text))
        
    dalen = pd.DataFrame({'datanodes':danamelist,'length':dalenlist,'lastid':daidlist,'size':dasizelist})
    dalen = dalen.sort_values('size',ascending = 0)
   
    plist = list(range(0,p))
    for i in range(0,rr-1):
        plist = plist + plist
    plist.sort()
    
    plocdict = dict()
    if (storeway == 'hashing'):
        i = 0
        for x in plist:
            requests.put(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]),json = '')
            plocdict.setdefault(x,[]).append(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]))
            dalen.iloc[i,2] = dalen.iloc[i,2]+1
            dalen.iloc[i,1] = dalen.iloc[i,1]+1
            dalen.iloc[i,3] = dalen.iloc[i,3] - psize
            i =  i + 1
            if (i == len(dalen)):
                i = 0
        
    #for x in range(0,len(dalen)):
    #   requests.put(url + 'data/{dnode}/usage(MB).json'.format(dnode = dalen.iloc[x,0]), json = dsize - dalen.iloc[x,3])
    
    #get partition location in data
    requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=inum), json = plocdict)
    
    try:
        dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i=pid))
        dres = json.loads(dr.text)
        if (dres == None):
            dres = []
    except:
        dres = []
    if (len(dres) == 0 or ()): # first child
        requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {'0':inum})
    else:
        requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {len(dres):inum})
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = pid)
    requests.patch(url_node,json = 
                      {'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
    return ('Success:Upload a file successfully~')


def put_json(fileloc,dir,p,storecal=0,storeway="hashing",user ='user',group = 'group'):
    ########## PERMISSION ############################
    ipath = dir
    perm = permission(ipath=ipath, user = user, group = group)
    if perm == 'everyone':
        print('Fail:no permission')
        return
    elif perm == 'group':
        print('Fail:no permission')
        return
    ##################################################
    #res3 is relationship between inodes
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)

    dirlist = dir.split('/')
    dirlist[0] ='/'
    while ('' in dirlist):
        dirlist.remove('')
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    if pid == 'fail':
        return('Fail:parent directory is incorrect')

    #check whether file has exited
    brother = ls(dir)
    filename = fileloc.split('/')[-1]
    if filename in brother:
        return('Fail:file with the same name exists, pls remove first') 

    #check whether put the file will be out of the range of datanode
    fsize = os.stat(fileloc).st_size #Byte
    fsize = fsize / (1000*1000) #MB
    
    dinfo = requests.get(url+'metadata/INodeSection.json')
    dinfo = json.loads(dinfo.text)
    
    rr = dinfo['replication']
    rdn = dinfo ['numdatanodes']
    dsize = dinfo['datanodesize(MB)']
    
    psize = fsize / p
    
    minsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToLast=1')
    minsize = json.loads(minsize.text)
    for x in minsize.values():
        minsize = dsize - x['usage(MB)']
    if minsize < psize * (math.floor(p * rr /rdn)):
        print('Fail: no space to store the file1')
        return
    
    mr = (p * rr) %rdn
    maxsize = requests.get(url+'data.json?orderBy="usage(MB)"&limitToFirst={maxnodenum}'.format(maxnodenum = mr))
    maxsize = json.loads(maxsize.text)
    for x in maxsize.values():
        if (dsize - x['usage(MB)']) < psize * (math.ceil(p * rr /rdn)):
            print('Fail: no space to store the file2')
            return
    
    res1 = dinfo['lastInodeId']
    inum = res1 + 1  
    
    with open(fileloc) as f:
        df = f.read()
    df = json.loads(df)
    
    df = list(df.items())
    
    #setup metadata
    if group == 'admin':
        pmson = '775'
    elif group == 'group':
        pmson = '777'
    else:
        pmson = '755'
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = str(inum))
    requests.patch(url_node,json = 
                       {'id':inum,
                        'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                        'name':filename,
                        'partition':p,
                        # default user->group, permission as 755
                        'owner':user,
                        'group':group,
                        'permission':pmson,
                        'storeway':storeway,
                        'storecal':storecal,
                        'type':'file',
                        'file_size':fsize, # not include replication size
                        'partition_size':psize,
                        'row_size':fsize/len(df)})
    requests.patch(url + 'metadata/INodeSection.json',json = {'lastInodeId':inum})
    r2 = requests.get(url+'metadata/INodeSection/numInodes.json')
    res2 = json.loads(r2.text)
    requests.patch(url+'metadata/INodeSection.json', json = {'numInodes':res2+1})
        
    #setup datanodes
     
    urllist = []
    for v in res3.values():
        if type(v[0]) == list:
            for x in range(0,len(v)):
                for z in range(0,len(v[x])):
                    urllist.append(v[x][z])
    
    udf = pd.DataFrame(columns=['datanode','partition'])
    for ui in range(0,len(urllist)):
        u = urllist[ui]
        u = u.rsplit('/',2)
        u[2] = u[2].split('.')[0]
        u.pop(0)
        udf.loc[ui]=u
        
     
    mdf = pd.DataFrame(udf.groupby(['datanode'],as_index=False).agg({'partition':[max,len]}))
    mdf = mdf.sort_values('datanode',ascending = 1)
 
    mdf_left = pd.DataFrame(['datanodes{inum}'.format(inum=i) for i in range(1,rdn+1)],columns=['datanode'])
    mdf = pd.merge(mdf_left,mdf,how='left',left_on='datanode',right_on='datanode')
    mdf = mdf.fillna(0)
    mdf.columns = ['datanodes','len','max']
  
    danamelist = list(mdf['datanodes'])
    dalenlist = list(mdf['max'])
    daidlist = []
    for x in range(0,len(mdf)):
        if int(mdf['max'][x]) == 0 & int(mdf['len'][x]) == 0:
            daidlist.append(0)
        else:
            daidlist.append(int(mdf['max'][x]))
    #setup datanodes
    dasizelist = []
    for s in range(1,rdn+1):
        dasizelist.append(dsize-float(requests.get(url+'data/datanodes{nums}/usage(MB).json'.format(nums = s)).text))
        
    dalen = pd.DataFrame({'datanodes':danamelist,'length':dalenlist,'lastid':daidlist,'size':dasizelist})
    dalen = dalen.sort_values('size',ascending = 0)
   
    plist = list(range(0,p))
    for i in range(0,rr-1):
        plist = plist + plist
    plist.sort()
    
    plocdict = dict()
    if (storeway == 'hashing'):
        i = 0
        for x in plist:
            df_temp = df[x:len(df):p]
            js_temp = dict((i,j) for i,j in df_temp)
            b = requests.put(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]),
                            json = js_temp)
            
            plocdict.setdefault(x,[]).append(url+ 'data/{datanodeid}/{lastid}.json'.format(datanodeid = dalen.iloc[i,0],lastid =dalen.iloc[i,2]))
            dalen.iloc[i,2] = int(dalen.iloc[i,2])+1
            dalen.iloc[i,1] = int(dalen.iloc[i,1])+1
            dalen.iloc[i,3] = int(dalen.iloc[i,3]) - psize
            i =  i + 1
            if (i == len(dalen)):
                i = 0
    for x in range(0,len(dalen)):
        requests.put(url + 'data/{dnode}/usage(MB).json'.format(dnode = dalen.iloc[x,0]), json = dsize - dalen.iloc[x,3])
    #get partition location in data
    requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=inum), json = plocdict)
    
    try:
        dr = requests.get(url+'metadata/INodeDirectorySection/{i}.json'.format(i=pid))
        dres = json.loads(dr.text)
        if (dres == None):
            dres = []
    except:
        dres = []
    if (len(dres) == 0 or ()): # first child
        b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {'0':inum})
    else:
        b = requests.patch(url + 'metadata/INodeDirectorySection/{i}.json'.format(i=pid), json = {len(dres):inum})
    print(b)
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = pid)
    a = requests.patch(url_node,json = 
                      {'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
   

def fast_getpartition(file):
    r3 = requests.get(url+'metadata/INodeDirectorySection.json')
    res3 = json.loads(r3.text)
    dirlist = file.split('/')
    dirlist[0] ='/'
    dirlist1 = dirlist
    pid = check_parent_child(res3, dirlist1)
    

    r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
    r1 = json.loads(r1.text)
    
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = r1[0])
    requests.patch(url_node,json = {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000)})
    
    loc = dict()
    for x in range(0,len(r1)):
        loc[x]=r1[x]  
    return (loc)

def fast_add(file, dict_add,put_p=0): #dict_add is in the format of dict, put_p must be 0 means only 1 partition
    if (type(dict_add) != dict):
        print('Fail: added record is not in the format of dict')
        return

    try:
        r3 = requests.get(url+'metadata/INodeDirectorySection.json')
        res3 = json.loads(r3.text)
        dirlist = file.split('/')
        dirlist[0] ='/'
        dirlist1 = dirlist
        pid = check_parent_child(res3, dirlist1)

        r1 = requests.get(url+'metadata/INodeDirectorySection/{inodeid}.json'.format(inodeid = pid))
        dict_pat = json.loads(r1.text)
    except:
        print('Fail: cannot getpartititon of file1')
        return
    try:
        if (dict_pat[0:4] == 'Fail'):
            print('Fail: cannot getpartititon of file2')
            return
    except:
        pass
    # readpartition
    # comfirm total rows of data: N -> the first data is N+1
    
    r = requests.get(url + 'metadata/INodeSection/inodes.json?orderBy="name"&equalTo="{nodename}"'.format(nodename = file.split('/')[-1]))
    r = json.loads(r.text)
    for x in r.keys():
        rsize = r[x]['row_size']
        fsize = r[x]['file_size']
   
    rdata = requests.get(dict_pat[put_p][0])
    size_p = len(json.loads(rdata.text))
    
    for url_x in dict_pat[put_p]:
        requests.put(url_x.replace('.json','/') + '{lastid}.json'.format(lastid = size_p),json = dict_add)
        usage_temp = requests.get(url_x.rsplit('/',1)[0] + '/usage(MB).json')
        requests.put(url_x.rsplit('/',1)[0] + '/usage(MB).json',json = float(usage_temp.text) + rsize )
   
    
    url_node = url + 'metadata/INodeSection/inodes/{i}.json'.format(i = pid)
    requests.patch(url_node,json = 
                   {'atime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                    'mtime':round((datetime.now() - datetime(1970, 1, 1)).total_seconds()*1000),
                    'file_size':fsize + rsize})
    


    