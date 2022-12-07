import streamlit as st
from command.Mapreduce_app import *
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

st.title(':eyes: Compass')
st.header("Search")

search_by = ["Movie Name", "Cast", "Time"]
user_select = st.selectbox(label= "Search by", options = search_by)

user_input_name = ""
user_input_time1, user_input_time2 = "", ""
if user_select != "Time":
    user_input_name = st.text_input("What's in your mind:")
else:
    user_input_time1 = st.text_input("From")
    user_input_time2 = st.text_input("To")


def aggrid_interactive_table(df: pd.DataFrame):
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )
    
    options.configure_default_column(min_column_width=200)
    options.configure_side_bar(filters_panel= True, columns_panel = True, defaultToolPanel = '')

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
    
    

c = st.container()
c.empty()

if user_input_name or (user_input_time1 and user_input_time2):
    if user_select ==  "Movie Name":
        df = movie_searcher(user_input_name)
    if user_select == "Cast":
        df = cast_searcher(user_input_name)
    if user_select == "Time":
        df = date_searcher(user_input_time1, user_input_time2)

    # "Time"
    if len(df):
        selection = aggrid_interactive_table(df)
        if len(selection["selected_rows"]):
            beauty_print_full_info(full_info_checker(selection["selected_rows"][0]["Movie ID"]))
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

    else:
        st.write("no result found")
