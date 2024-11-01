######################
# Constants
DUCKDB_PATH = 'my-dagster-project/my_dagster_project/database/youtube'

#######################
# Import libraries
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import duckdb
import re
from datetime import timedelta, datetime

#######################
# Page configuration
st.set_page_config(
    page_title="Youtuber Dashboard",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

#######################
# CSS styling
st.markdown("""
<style>

[data-testid="block-container"] {
    padding-left: 2rem;
    padding-right: 2rem;
    padding-top: 1rem;
    padding-bottom: 0rem;
    margin-bottom: -7rem;
}

[data-testid="stVerticalBlock"] {
    padding-left: 0rem;
    padding-right: 0rem;
}

[data-testid="stMetric"] {
    background-color: #393939;
    text-align: center;
    padding: 15px 0;
}

[data-testid="stMetricLabel"] {
  display: flex;
  justify-content: center;
  align-items: center;
}

[data-testid="stMetricDeltaIcon-Up"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

[data-testid="stMetricDeltaIcon-Down"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

</style>
""", unsafe_allow_html=True)


#######################
# Plots


# Donut chart
def make_donut(input_response, input_text, input_color):
  if input_color == 'blue':
      chart_color = ['#29b5e8', '#155F7A']
  if input_color == 'green':
      chart_color = ['#27AE60', '#12783D']
  if input_color == 'orange':
      chart_color = ['#F39C12', '#875A12']
  if input_color == 'red':
      chart_color = ['#E74C3C', '#781F16']
    
  source = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100-input_response, input_response]
  })
  source_bg = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100, 0]
  })
    
  plot = alt.Chart(source).mark_arc(innerRadius=45, cornerRadius=25).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          #domain=['A', 'B'],
                          domain=[input_text, ''],
                          # range=['#29b5e8', '#155F7A']),  # 31333F
                          range=chart_color),
                      legend=None),
  ).properties(width=130, height=130)
    
  text = plot.mark_text(align='center', color="#29b5e8", font="Lato", fontSize=22, fontWeight=700, fontStyle="italic").encode(text=alt.value(f'{input_response} %'))
  plot_bg = alt.Chart(source_bg).mark_arc(innerRadius=45, cornerRadius=20).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          # domain=['A', 'B'],
                          domain=[input_text, ''],
                          range=chart_color),  # 31333F
                      legend=None),
  ).properties(width=130, height=130)
  return plot_bg + plot + text



# Convert population to text 
def format_number(num):
    if num >= 1000000000:
        if not num % 1000000000:
            return f'{num // 1000000000} B'
        return f'{round(num / 1000000000, 1)} B'
    elif num >= 1000000:
        if not num % 1000000:
            return f'{num // 1000000} M'
        return f'{round(num / 1000000, 1)} M'
    elif num >= 1000:
        return f'{num // 1000} K'
    return f'{num}'


def parse_to_datetime(time_str):
    # Using regex to capture numbers and time units
    time_regex = r'(\d+)\s+(hour|minute|second|day|week|month|year)s?\s+ago'
    
    # Initialize timedelta components
    time_values = {'hours': 0, 'minutes': 0, 'seconds': 0, 'days': 0, 'weeks': 0, 'months': 0, 'years': 0}
    
    # Extract all matches from the string
    for match in re.finditer(time_regex, time_str):
        value, unit = match.groups()
        time_values[unit + 's'] = int(value)
    
    # Calculate total timedelta from extracted values
    total_time = timedelta(
        days=time_values['days'] + 
             (time_values['weeks'] * 7) + 
             (time_values['months'] * 30) +  # Approximate each month as 30 days
             (time_values['years'] * 365),
        hours=time_values['hours'],
        minutes=time_values['minutes'],
        seconds=time_values['seconds']
    )
    
    # Subtract timedelta from the current time to get an absolute datetime
    absolute_date = datetime.now() - total_time
    
    return absolute_date


#######################
# Load data
conn = duckdb.connect(DUCKDB_PATH)
metadata = conn.execute("SELECT * FROM metadata").fetchdf()
thumbnails = conn.execute("SELECT * FROM thumbnails").fetchdf()
conn.close()

thumbnails['time'] = thumbnails['time_updated'].apply(parse_to_datetime)

#######################
# Sidebar
with st.sidebar:
    st.title('🏂 Youtuber Dashboard')
    
    youtuber_list = list(metadata.channel_tag.unique())
    
    selected_youtuber = st.selectbox('Select a youtuber', youtuber_list)

    df_selected_youtuber = metadata[metadata.channel_tag == selected_youtuber]
    df_selected_youtuber_trends = thumbnails[thumbnails.channel_tag == selected_youtuber]

    df_selected_youtuber_growth = df_selected_youtuber_trends.groupby(df_selected_youtuber_trends['time'].dt.year)['view_count'].sum()


    # color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    # selected_color_theme = st.selectbox('Select a color theme', color_theme_list)


#######################
# Dashboard Main Panel
col = st.columns((1.5, 4.5, 2), gap='medium')


with col[0]:
    st.markdown(f'#### {selected_youtuber}')
    total_videos = metadata[metadata['channel_tag'] == selected_youtuber]['video_count'].values[0]
    total_subscribers = format_number(metadata[metadata['channel_tag'] == selected_youtuber]['subscriber_count'].values[0])
    total_views = format_number(metadata[metadata['channel_tag'] == selected_youtuber]['view_count'].values[0])

    st.metric(label='Total Videos', value=total_videos)
    st.metric(label="Sub Count", value=total_subscribers)
    st.metric(label="Total Views", value=total_views)


    longform_videos = thumbnails.groupby('channel_tag')['channel_tag'].count()[selected_youtuber]
    shortform_videos = total_videos - longform_videos

    longform_videos_percentage = make_donut(round((longform_videos/total_videos)*100, 0), 'Videos', 'red')
    shortform_videos_percentage = make_donut(round((shortform_videos/total_videos)*100, 0), 'Shorts', 'blue')

    migrations_col = st.columns((0.2, 1, 0.2))
    with migrations_col[1]:
        st.write('Videos')
        st.altair_chart(longform_videos_percentage)
        st.write('Shorts')
        st.altair_chart(shortform_videos_percentage)

with col[1]:
    st.markdown('#### Views by each video')

    st.line_chart(data=df_selected_youtuber_trends, y='view_count', color='#ffffff')

    st.markdown('#### Top viewed videos')

    st.dataframe(data=df_selected_youtuber_trends.sort_values(by='view_count', ascending=False).head(7),
                 column_order=['video_title', 'view_count'],
                 hide_index=True,
                 use_container_width=True)

with col[2]:
    st.markdown('#### Views by year')

    st.bar_chart(data=df_selected_youtuber_growth,  y='view_count', horizontal=True)

    with st.expander('About', expanded=True):
        st.write(f'''
            - Joined: {metadata[metadata.channel_tag == selected_youtuber]['joined_date'].astype(str).values[0]}.
            - :orange[**Is verified**]: {metadata[metadata.channel_tag == selected_youtuber]['is_verified'].values[0]}
            - :orange[**Channel tag**]: @{ metadata[metadata.channel_tag == selected_youtuber]['channel_tag'].values[0]}
            - :orange[**Channel Name**]: { metadata[metadata.channel_tag == selected_youtuber]['channel_name'].values[0]}
            ''')
        
    st.image(df_selected_youtuber_trends.sort_values(by='view_count', ascending=False)['thumbnail_link'].values[0])
