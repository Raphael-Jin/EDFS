import streamlit as st
from command.Mapreduce_app import *

# pip install streamlit-aggrid
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import numpy as np

st.title(':chart_with_upwards_trend: Trending')

st.header("Popular Movies")

movie_type = ["Action", "Horror", "Comedy", 'Drama', 'History', 'Science Fiction', 'Crime']
order_by = ["Popularity", "Year"]
Desc = ["Descending", "Ascending"]
result_number = [10, 20, 50, 100, 200]
col1, col2, col3, col4 = st.columns(4)

user_type = col1.selectbox(label= "select a movie type", options = movie_type)
user_order_by = col2.selectbox(label= "Order By", options = order_by)
user_desc = col3.selectbox(label = "Desc or Asc", options = Desc)
user_result_number = col4.selectbox(label= "Result number", options = result_number)

def aggrid_interactive_table(df: pd.DataFrame):
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )
    
    options.configure_default_column(min_column_width=200)
    options.configure_selection(selection_mode="single")
    options.configure_pagination(paginationAutoPageSize=False,paginationPageSize=10)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        # theme="light",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load= True
    )
    return selection

def beauty_print_full_info(full_info):
    exp = st.expander(selection["selected_rows"][0]["Title"])
    # single res
    full_info = full_info.values()
    with exp:
        for single_movie_info in full_info:
            st.subheader("Overview")
            single_movie_info["overview"]
            col1, col2, col3, col4 = st.columns(4)

            col1.metric(label = "Rating Average", value = single_movie_info["vote_average"], delta=None, delta_color="normal", help=None)
            col2.metric(label = "Rating Count", value = single_movie_info["vote_count"], delta=None, delta_color="normal", help=None)
            col3.metric(label = "Popularity", value = round(single_movie_info["popularity"], 2), delta=None, delta_color="normal", help=None)
            col4.metric(label = "Language", value = single_movie_info["original_language"], delta=None, delta_color="normal", help=None)

            import ast
            st.subheader("Keywords")
            v = str(single_movie_info["keywords"])
            this_list = ast.literal_eval(v)
            st.write(", ".join(this_list))

            st.subheader("Cast")
            v = str(single_movie_info["cast"])
            this_list = ast.literal_eval(v)
            st.write(", ".join(this_list))

            st.subheader("Director")
            v = str(single_movie_info["director"])
            st.write(v)
    this_url = single_movie_info["homepage"]
    if this_url != "Unknown":
        import streamlit.components.v1 as components
        components.iframe(this_url,height=700)

res_df = movie_filter_n_rank(movie_type = user_type, order_by = user_order_by ,N = user_result_number, Desc = True if (user_desc == "Descending") else False)
    
selection = aggrid_interactive_table(df=res_df)

if len(selection["selected_rows"]):
    user_choose_id = selection["selected_rows"][0]["Movie ID"]
    beauty_print_full_info(full_info_checker(user_choose_id))
    with st.form("my_form"):
        slider_val = st.slider("Rating", min_value=1, max_value=5)
        submitted = st.form_submit_button("Submit")

        if submitted:
            if selection:
                import time
                # ts stores the time in seconds
                ts = time.time()
                dic = {"movieId":selection["selected_rows"][0]["Movie ID"],
                    "rating":slider_val,
                    "timestamp":ts,  
                    "userId": st.session_state["shared"]["user_id"]
                }
                addpartition('/user/user_data/custom/rating/rating_'+st.session_state["shared"]["user_id"] + ".csv", dic)
                st.write("You rates " + str(slider_val) + " for: ") 
                st.write(selection["selected_rows"][0]["Title"])

st.header("Popular Actors")
actor_df = actor_rank()

import plotly.express as px 
fig=px.bar(actor_df,x='Popularity',y='Actor', orientation='h', color= "Popularity", color_continuous_scale=px.colors.sequential.Viridis) 
fig.update_layout(yaxis=dict(autorange="reversed"))
st.write(fig)

st.header("Popular Directors")
director_df = director_rank()
fig=px.bar(director_df,x='Popularity',y='Director', orientation='h', color= "Popularity")
fig.update_layout(yaxis=dict(autorange="reversed"))
st.write(fig)