import streamlit as st  # pip install streamlit
from command.fscommand import cat, addpartition, mkdir, put_efile

# pre-stroed matrix
if "user_like" and "co_occurance_matrix" not in st.session_state:
    import command.Mapreduce_app as mpr
    st.session_state.co_occurance_matrix = mpr.read_exist_matrix()

if not "new" in st.session_state:
    st.session_state["new"] = ''
if not "stauth" in st.session_state:
    st.session_state["stauth"] = None
if "vcode" not in st.session_state:
   st.session_state["vcode"] = ''
if "shared" not in st.session_state:
   st.session_state["shared"] = {}
if "way" not in st.session_state:
   st.session_state["way"] = ''
if "path" not in st.session_state:
   st.session_state["path"] = '/'
if "ini_path" not in st.session_state:
   st.session_state["ini_path"] = ''
if not 'flag' in st.session_state:
    st.session_state['flag'] = ''

# emojis: https://www.webfx.com/tools/emoji-cheat-sheet/
st.set_page_config(page_title="Movie", page_icon=":clapper:", layout="wide")

hide_bar= """
    <style>
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
        visibility:hidden;
        width: 0px;
    }
    [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
        visibility:hidden;
    }
    </style>
"""
del st.session_state['path']
del st.session_state['ini_path']

if st.session_state['new'] == 'new':
    st.success('Register successfully~')
 
if (st.session_state['stauth'] == None) or (st.session_state['stauth'] == False):
    with st.form("login_form"):
        st.header('Login')
        st.text_input('Username',key = "inputname")
        st.text_input('Password',key = "inputpwd",type = 'password')
        def logined():
            # mapreduce
            df = cat('/user/user_data/user_info.csv')        
            usernames = df['username']
            if (st.session_state["inputname"]!='') & (st.session_state["inputpwd"]!=''):
                if st.session_state["inputname"] in list(usernames):
                    if st.session_state["inputpwd"] in list(df.loc[df['username']==st.session_state["inputname"],'password']):
                        st.session_state["stauth"] = True
                        st.session_state["shared"]["username"] = st.session_state["inputname"]

                        # add here
                        st.session_state["shared"]["user_id"] = df.loc[df['username']==st.session_state["inputname"],'userid'].to_string(index=False)

                        st.session_state["shared"]["group"] = df.loc[df['username']==st.session_state["inputname"],'group'].to_string(index=False)
                        if st.session_state['new'] != '':
                            st.session_state['new'] = ''
                        st.balloons()
                    else:
                        st.session_state["stauth"] = False
                else:
                    st.session_state["stauth"] = False
        logined = st.form_submit_button("Login",on_click=logined)
            
if st.session_state["stauth"] == False:
    st.error("Username/password is incorrect")
    st.markdown(hide_bar, unsafe_allow_html=True)

if st.session_state["stauth"] == None:
    st.warning("Please enter your username and password")
    st.markdown(hide_bar, unsafe_allow_html=True)
    
if (st.session_state["stauth"] == None) | (st.session_state["stauth"] == False):
    op = st.selectbox("Find a way to login",index= 0,key ='way',options = ['','Find password','Register'])
    if op == 'Find password':
        with st.form("findpwd_form"):
            st.text_input('Username',key = 'pwd_username')
            st.text_input('Email',key = 'pwd_email')
            submitted = st.form_submit_button("Submit")
            if submitted:
                df = cat('/user/user_data/user_info.csv') 
                if st.session_state["pwd_email"] in list(df.loc[df['username']==st.session_state["pwd_username"],'email']):
                    st.info('Your username: {uname}, password: {pwd}'.format(uname=st.session_state["pwd_username"],pwd=df.loc[df['username']==st.session_state["pwd_username"],'password'].to_string(index=False)))
                    st.session_state['new'] = 'find'
                else:
                    st.error("Username/Email is incorrect")
        if st.session_state['new'] == 'find':
            st.button('Login')
            del st.session_state['way']
            
    elif op == 'Register':
        with st.form("register_form"):
            name_val = st.text_input('Name')
            username_val = st.text_input('Username')
            email_val = st.text_input('Email')
            pwd_val = st.text_input('Password',type='password')
            group = st.selectbox("Group",options=['','admin','user'],key='group')
            submitted = st.form_submit_button("Submit")
            if submitted:
                if (st.session_state['vcode'] == 'admin') or (st.session_state['group'] == 'user'):
                    df = cat('/user/user_data/user_info.csv')
                    dic = {"group":group,"password": pwd_val,"user":name_val,"userid":max(df['userid'])+1,
                       "username":username_val,"email":email_val}
                    
                    st.session_state['new'] = 'new'
                    addpartition('/user/user_data/user_info.csv', dic)
                    if (st.session_state['group'] == 'user'):
                        mkdir('/user/'+username_val,user=username_val,group=username_val,force_allowed = True)
                        put_efile('rating_{userid}.csv'.format(userid = max(df['userid'])+1),
                                  '/user/user_data/custom/rating',fsize=0,p=1,storecal=0,storeway="hashing",user ='user',group = 'group')
                    del st.session_state['way']
                    st.experimental_rerun()
                elif (st.session_state['vcode'] == '') and (st.session_state['group'] == 'admin'):
                    st.text_input("Put validation code:",key="vcode",type='password')
                else:
                    st.error("No admin permission")


if st.session_state["stauth"]:
    # # ---- SIDEBAR ----
    st.sidebar.title("Welcome {username}".format(username = st.session_state["shared"]["username"]))
    # st.sidebar.header("select page here :")
    st.title("Welcome to Movie Info Storage & Recommender system:v:")

    st.markdown(''' _**Movie Information Storage and Recommendation System (MISR)**_ is a distributed system. 
            Information and recommendations for different movie can be got here. A private file space is also available here. 
            ''', unsafe_allow_html=True)
    st.markdown(':point_left:Select a page from dropdown on the left to see what we can do! _**Enjoy your movie time~**_', unsafe_allow_html=True)   
    st.markdown("## Introduction")
    st.markdown('### :open_file_folder: File System')
    st.write('''* _**File System**_ is a distributed-based place for users to add, delete, access, and manage files. 
            Users can have private space here. Only CSV and JSON uploads are currently supported.
            ''')
    st.markdown('### :chart_with_upwards_trend: Trending')
    st.write('''* _**Trending**_ will provide you with the most popular or newest movie, 
            as well as the most popular actors or directors.
            ''')

    st.markdown('### :eyes: Compass')
    st.write('''* _**Compass**_ allows you to serach the movie's information whatever you are interested in.
            It also allow you to rates them.
            ''')
    
    st.markdown('### :bulb: Inspiration')
    st.write('''* _**Inspiration**_ will automatically analysis your rating history and give you recommendations based on them.
            ''')
    
    st.markdown("## Acknowledgements")
    st.write('''* _**Kaggle - The Movies Dataset:**_ This dataset consists of metadata from over 45,000 movies and 26 million ratings from over 270,000 users. 
             More information can be accessed [here](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset).''')
    st.write('''* _**Streamlit:**_ Streamlit is an open-source Python library that makes it easy to create and share beautiful, 
             custom web apps for machine learning and data science. More information can be accessed [here](https://streamlit.io/).''')
    st.sidebar.success("Select a page above.")
        
    def logouted():
        st.session_state["stauth"] = None
        st.session_state["shared"] = {}
    logouted = st.sidebar.button("Logout",on_click = logouted)
