import streamlit as st
import pandas as pd
from command.fscommand import browse, put_st, mkdir_st, rm, cat, getpartition, readpartition

if not "path" in st.session_state:
    if st.session_state["shared"]["group"] == "user":
        st.session_state["path"] = '/user/' + st.session_state["shared"]["username"]
        if not 'ini_path' in st.session_state:
            st.session_state['ini_path'] = st.session_state["path"]
    else:
        st.session_state["path"] = '/'
        if not 'ini_path' in st.session_state:
            st.session_state['ini_path'] = st.session_state["path"]

if not 'result' in st.session_state:
    st.session_state['result'] = ''
if not 'flag' in st.session_state:
    st.session_state['flag'] = ''
if not 'partition' in st.session_state:
    st.session_state['partition'] = ''
if not 'dirname' in st.session_state:
    st.session_state['dirname'] = ''
if not 'rt' in st.session_state:
    st.session_state['rt']=''

st.title(':open_file_folder:File System')
st.subheader("Browse Directory")
st.write('Current Directory:')
path = st.text_input('',st.session_state['path'],label_visibility='collapsed',disabled=True)
if st.session_state["shared"]["group"] == 'user':
    rt, df = browse(path,st.session_state["shared"]["username"],st.session_state["shared"]["username"]) #browse need user and group
    st.session_state['rt'] = rt
else:
    rt, df = browse(path,st.session_state["shared"]["username"],st.session_state["shared"]["group"])
    st.session_state['rt'] = rt

if (rt !='file') & (df.empty == False):
    st.write("Directory Content:")
    st.table(df[['id','name','mtime','group','owner','permission','type','goto']])
    #next
    cname = list(df['goto'])
    cname.insert(0,'choose next directory')
elif (rt != 'file') & (df.empty == True):
    st.write("Directory Content:")
    st.table(pd.DataFrame())
    cname = ['No sub-directory']
elif rt == 'file':
    st.write("File Summary:")
    with st.expander("File Information"):
        col1,col2 = st.columns([1, 1])
        with col1:
            st.write('* _**nodeid:**_ ' + df['id'].to_string(index=False))
            st.write('* _**name:**_ ' + df['name'].to_string(index=False))
            st.write('* _**owner:**_ ' + df['owner'].to_string(index=False))
        with col2:
            st.write("* _**group:**_ " + df['group'].to_string(index=False))
            st.write("* _**permission:**_ " + df['permission'].to_string(index=False))
            st.write("* _**type:**_ " + df['type'].to_string(index=False))

    with st.expander("File Activity"):
        col1,col2 = st.columns([1, 1])
        with col1:
            st.write("* _**last access time:**_ " + df['atime'].to_string(index=False))
        with col2:
            st.write("* _**last modified time:**_ " + df['mtime'].to_string(index=False))

    with st.expander("File Storage"):
        col1,col2 = st.columns([1, 1])
        with col1:
            st.write("* _**size(MB):**_ " + df['file_size'].to_string(index=False))
            st.write("* _**partition:**_ ", df['partition'].to_string(index=False))
        with col2:
            st.write("* _**storeway:**_ " + df['storeway'].to_string(index=False))
    cname = [str(st.session_state['path'])]
cname = tuple(cname)

# back
if (st.session_state['path'] != st.session_state['ini_path']) & (st.session_state['ini_path'] != '/'):
    ini_c = st.session_state['ini_path'].count('/')
    def parent_path(path):
        pname = list()
        for c in range(0,path.count('/')-ini_c):
            pname.append(path.rsplit('/',c+1)[0])
        return pname
    pname = parent_path(st.session_state['path'])
    pname.insert(0,'choose parent directory')
elif (st.session_state['path'] != st.session_state['ini_path']) & (st.session_state['ini_path'] == '/'):
    def parent_path(path):
        pname = list()
        for c in range(0,path.count('/')):
            pname.append(path.rsplit('/',c+1)[0])
        return pname
    pname = parent_path(st.session_state['path'])
    pname[-1] = '/'
    pname.insert(0,'choose parent directory')
else:
    pname = ['No parent-directory']
pname = tuple(pname)

col1, col2 = st.columns([1, 1])
with col1:
    def change_path_back():
            st.session_state['path'] = st.session_state['back']
    st.selectbox('Back:', pname, key ='back', on_change=change_path_back)
with col2:
    def change_path_next():
            st.session_state['path'] = st.session_state['next']
    st.selectbox('Next:', cname, key ='next', on_change=change_path_next)

st.subheader("")
if rt != 'file':
    st.subheader("Actions on current directory")
    tab1, tab2, tab3 = st.tabs(["Upload a file", "Make a new directory", "Remove a directory"])
    with tab1:
        with st.form('upload_file'):
            pnum = st.text_input('Partition number:',key = 'partition')
            uploaded_file = st.file_uploader("Choose a file:",key='upfile')
            submitted = st.form_submit_button("Submit")
            if submitted:
                if (uploaded_file is not None) & (int(pnum)>0):
                    if st.session_state["shared"]["group"] == 'user':
                        r = put_st(uploaded_file,uploaded_file.name,uploaded_file.size,st.session_state["path"],
                           p = int(st.session_state["partition"]),storecal=0,storeway="hashing",
                           user = st.session_state["shared"]["username"],group = st.session_state["shared"]["username"])
                    else:
                        r = put_st(uploaded_file,uploaded_file.name,uploaded_file.size,st.session_state["path"],
                           p = int(st.session_state["partition"]),storecal=0,storeway="hashing",
                           user = st.session_state["shared"]["username"],group = st.session_state["shared"]["group"])
                    if r[0] != 'S':
                        st.error(r)
                    else:
                        st.session_state['flag'] = 'success1'
                        st.session_state['result'] = r
                        st.experimental_rerun()
                elif int(pnum)<=0:
                    st.error('Fail:Partition number must be greater than 0')
                elif uploaded_file is not None:
                    st.error('Fail:Upload a file first')
            if st.session_state['flag'] == 'success1':
                st.success(st.session_state['result'])
                st.session_state['result'] =''
                st.session_state['flag'] = ''
    with tab2:
        with st.form('make_dir'):
            dirname = st.text_input('New directory name:',key = 'dirname')
            dirloc = st.session_state['path'] + '/' + st.session_state['dirname']
            submitted = st.form_submit_button("Submit")
            if submitted:
                if dirname != '':
                    if st.session_state["shared"]["group"] == 'user':
                        r = mkdir_st(dirloc,user =st.session_state["shared"]["username"],
                                 group = st.session_state["shared"]["username"])
                    else:
                        r = mkdir_st(dirloc,user =st.session_state["shared"]["username"],
                                 group = st.session_state["shared"]["group"])
                    if r[0] != 'S':
                        st.error(r)
                    else:
                        st.session_state['flag'] = 'success2'
                        st.session_state['result'] = r
                        st.experimental_rerun()
                else:
                    st.error('Fail:Give the new directory a name first')
            if st.session_state['flag'] == 'success2':
                st.success(st.session_state['result'])
                st.session_state['result'] =''
                st.session_state['flag'] = ''
    with tab3:
        with st.form('rm_fd'):
            rname = list(cname)
            rname[0] = ''
            option = st.selectbox('Remove following file/directory',rname)
            submitted = st.form_submit_button("Submit")
            if submitted:
                if option != '':
                    if st.session_state["shared"]["group"] == 'user':
                        r = rm(option,user = st.session_state["shared"]["username"],group = st.session_state["shared"]["username"])
                    else:
                        r = rm(option,user = st.session_state["shared"]["username"],group = st.session_state["shared"]["group"])
                    if r[0] !='S':
                        st.error(r)
                    else:
                        st.session_state['flag'] ='success3'
                        st.session_state['result'] = r
                        st.experimental_rerun()
                else:
                    st.error('Fail:Choose one directory/file to remove')
            if st.session_state['flag'] == 'success3':
                st.success(st.session_state['result'])
                st.session_state['result'] =''
                st.session_state['flag'] = ''                        
else:
    st.subheader("Actions on current file")
    tab4, tab5, tab6 = st.tabs(["Show the file", "Get partition locations", "Read one partition"])
    with tab4:
        genre = st.radio("Choose display Format:",('csv','json'),horizontal=True)
        if genre =='csv':
            if st.session_state["shared"]["group"] == 'user':
                r1 = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                        group = st.session_state["shared"]["username"])
            else:
                r1 = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                        group = st.session_state["shared"]["group"])
            if type(r1) == str:
                st.error(r1)
            else:
                st.dataframe(r1)
                st.session_state['flag'] = 'success4'
        else:
            if st.session_state["shared"]["group"] == 'user':
                r1 = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                        group = st.session_state["shared"]["username"],force_convert_to_json=True)
            else:
                r1 = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                        group = st.session_state["shared"]["group"],force_convert_to_json=True)
            if type(r1) == str:
                st.error(r1)
            else:
                st.json(r1)
                st.session_state['flag'] = 'success4'
                
        if st.session_state['flag'] == 'success4':
            if genre == 'csv':
                def convert_df():
                    if st.session_state["shared"]["group"] == 'user':
                        df = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                                group = 'st.session_state["shared"]["group"]')
                    else:
                        df = cat(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                                group = st.session_state["shared"]["group"])
                    if type(df) == str:
                        return df
                    else:
                        df = df.to_csv().encode('utf-8')
                        return df
                df = convert_df()
                if type(df) == str:
                    pass
                else:
                    fname = st.session_state['path'].rsplit('/',1)[1]
                    if fname[-1] == 'n':
                        fname = fname.replace('.json','.csv')
                    st.download_button(label="Download file", data=df, file_name=fname, mime='text/csv')
            else:
                pass
            
    with tab5:
        if st.session_state["shared"]["group"] == 'user':
            r2 = getpartition(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                    group = st.session_state["shared"]["username"])
        else:
            r2 = getpartition(st.session_state['path'] ,user = st.session_state["shared"]["username"], 
                    group = st.session_state["shared"]["group"])
        if type(r2) == str:
            st.error(r2)
        else:
            st.json(r2)
    with tab6:
        if type(r2) == str:
            st.error(r2)
        else:    
            with st.form('read_par'):     
                ppnum = st.selectbox('Partition id:',r2.keys())
                pgen = st.radio("Choose display Format:",('csv','json'),horizontal=True)
                submitted = st.form_submit_button("Submit")
                if submitted:
                    if ppnum in r2.keys():
                        if pgen =='csv':
                            if st.session_state["shared"]["group"] == 'user':
                                r3 = readpartition(st.session_state['path'],ppnum,st.session_state["shared"]["username"],
                                                   st.session_state["shared"]["username"])
                            else:
                                r3 = readpartition(st.session_state['path'],ppnum,st.session_state["shared"]["username"],
                                                   st.session_state["shared"]["group"])
                            if type(r3) == str:
                                st.error(r3)
                            else:
                                st.dataframe(r3)
                                #st.session_state['flag'] = 'success6'
                        else:
                            if st.session_state["shared"]["group"] == 'user':
                                r3 = readpartition(st.session_state['path'],ppnum,st.session_state["shared"]["username"],
                                                   st.session_state["shared"]["username"],True)
                            else:
                                r3 = readpartition(st.session_state['path'],ppnum,st.session_state["shared"]["username"],
                                                   st.session_state["shared"]["group"],True)
                            if type(r3) == str:
                                st.error(r3)
                            else:
                                st.json(r3)
                                #st.session_state['flag'] = 'success6'
                    else:
                        st.error('Fail:Partition id is out of range')
    
        
