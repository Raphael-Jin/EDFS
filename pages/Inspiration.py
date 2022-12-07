import streamlit as st
from command.Mapreduce_app import *
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

from wordcloud import WordCloud
import matplotlib.pyplot as plt

this_id = st.session_state["shared"]["user_id"]

st.title(':bulb: Inspiration')

def beauty_print_full_info(full_info):
    exp = st.expander(selection["selected_rows"][0]["title"])
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
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load= True
    )
    return selection

if this_id:
    
    res = fast_cat('/user/user_data/custom/rating/rating_'+st.session_state["shared"]["user_id"] + ".csv")

    user_like = {}
    user_like_list = []
    user_rating_list = []

    if res:
        st.header("Those are for you, " + st.session_state["shared"]["username"])
        st.subheader("Base on these rates:")
        for one_record in res:
            user_like[str(one_record["movieId"])] = one_record["rating"]
            user_like_list.append(one_record["movieId"])
            user_rating_list.append(one_record["rating"])
        
        full_info_res = full_info_checker(user_like_list)
        order_list = []
        exp = st.expander("Your Rating History")
        for one_record in res:
            exp.write(full_info_res[one_record["movieId"]]["title"])
            star_arr = [":star:", ":star: :star: ",":star: :star: :star:",":star: :star: :star: :star:",":star: :star: :star: :star: :star:"]
            exp.markdown(body=star_arr[int(one_record["rating"]-1)])
        
        st.subheader("We Recommend:")
        if this_id:
            res_df = ItemBasedFiltering(user_like, st.session_state.co_occurance_matrix, user_id = this_id,train=False)
            slection_df = res_df[["id", "title", "genres", "cast"]]
            frequency_dic = dict(zip(res_df.title, res_df.frequency))
            st.set_option('deprecation.showPyplotGlobalUse', False)

            wordcloud = WordCloud(background_color="white")
            wordcloud.generate_from_frequencies(frequencies=frequency_dic)
            plt.figure()
            plt.imshow(wordcloud, interpolation="bilinear")
            plt.axis("off")
            plt.show()
            st.pyplot()

            selection = aggrid_interactive_table(slection_df)

            if len(selection["selected_rows"]):
                user_choose_id = selection["selected_rows"][0]["id"]
                beauty_print_full_info(full_info_checker(user_choose_id))
                
    else:
        st.header("You have not yet rates a movie, Start rates one, " + st.session_state["shared"]["username"] + " !")

