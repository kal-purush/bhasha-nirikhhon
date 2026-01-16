import os
import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector as db
import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
import mysql.connector


def api_connect():
 
    api="AIzaSyCTOOe0MX5-158N4TA5C_E2dFXsd7H_wcg"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version,developerKey = api)
    return youtube
youtube=api_connect()


connection =mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="root",
    database= "youtube"
)
cursor =connection.cursor()



channel_id='UCEgCIzCmg20cW_CWsslN1Kg'
def get_channel_info(channel_id):
    cursor.execute('''CREATE TABLE IF NOT EXISTS Channel_info (
                           Channel_Name varchar(255),
                           Channel_Id varchar(255),
                           Subscribers BIGINT,
                           View_Count BIGINT,
                           Playlist_id varchar(100),
                           Total_videos INT,
                           Description Text)''')
    
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
    
    for item in response.get('items', []):
        data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_Id': channel_id,
            'Subscribers': item['statistics']['subscriberCount'],
            'View_Count': item['statistics']['viewCount'],
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
            'Total_videos': item['statistics']['videoCount'],
            'Description': item['snippet']['description']
        }
       
        channel_details = (data['Channel_Name'], data['Channel_Id'], data['Subscribers'], data['View_Count'], data['Playlist_id'], data['Total_videos'], data['Description'])
        insert_query = ("INSERT INTO Channel_info (Channel_Name, Channel_Id, Subscribers, View_Count, Playlist_id, Total_videos, Description) VALUES (%s, %s, %s, %s, %s, %s, %s)")
        cursor.execute(insert_query, channel_details)
        connection.commit()
        
    return data


def get_channel_videos(channel_id):
    video_ids = []

    request = youtube.channels().list(
                   id=channel_id,
                   part='contentDetails')
    response1=request.execute()
    playlist_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token)
        response2=request.execute()
  
        for i in range(len(response2['items'])):
            video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response2.get('nextPageToken')

        if next_page_token is None:
            break
    cursor.execute("""CREATE TABLE IF NOT EXISTS video_ids (
                        video_id VARCHAR(255)
                    )""") 
    for video_id in video_ids:
        cursor.execute("INSERT INTO video_ids (video_id) VALUES (%s)", (video_id,))
    connection.commit()
    
    return video_ids

def get_video_details(video_ids):
    video_data = []
    
    try:
        for video_id in video_ids:
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS Videos_info(
                                   Channel_Name varchar(255),
                                   Channel_Id varchar(255),
                                   Video_id varchar(255),
                                   Title varchar(255),
                                   Views bigint,
                                   Likes bigint,
                                   Comments bigint,
                                   Thumbnail varchar(255),
                                   published_at varchar(100),
                                   Duration varchar(100),
                                   Description Text)''')

            for item in response.get('items', []):
                data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_id': item['id'],
                    'Title': item['snippet']['title'],
                    'Views': item['statistics']['viewCount'],
                    'Likes': item['statistics']['likeCount'],
                    'Comments': item['statistics']['commentCount'],
                    'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'published_at': item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                    'Duration': item['contentDetails']['duration'].replace('PT', ' '),
                    'Description': item['snippet']['description']
                }
                video_data.append(data)

                cursor.execute("INSERT INTO Videos_info (Channel_Name, Channel_Id, Video_id, Title, Views, Likes, Comments, Thumbnail, published_at, Duration, Description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (data['Channel_Name'], data['Channel_Id'], data['Video_id'], data['Title'], data['Views'], data['Likes'], data['Comments'], data['Thumbnail'], data['published_at'], data['Duration'], data['Description']))
                connection.commit()
                
    except Exception as e:
        print("Error:", e)            
    return video_data

#getting the comments details

def get_comments_details(video_ids):
    comment_data=[]
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS comment_info (
                            comment_Id VARCHAR(255),
                            video_Id VARCHAR(255),
                            comment_Text TEXT,
                            comment_Author VARCHAR(255),
                            comment_Published VARCHAR(255)
                        )""")
        
        for video_id in video_ids:
            request=youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                         video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''))

                comment_data.append(data)

                cursor.execute("INSERT INTO comment_info(comment_Id, video_Id, comment_Text, comment_Author, comment_Published) VALUES (%s, %s, %s, %s, %s)",
                               (data['comment_Id'], data['video_Id'], data['comment_Text'], data['comment_Author'], data['comment_Published']))
                connection.commit()
    except Exception as e:
        print(f"Error: {e}")
    
    return comment_data

#function to call the channel information

def channel_info(channel_id):
    channel_details=get_channel_info(channel_id)
    video_Ids=get_channel_videos(channel_id)
    video_details=get_video_details(video_Ids)
    comment_details=get_comments_details(video_Ids)

    channel_df = pd.DataFrame([channel_details])
    video_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)

    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
    }

#codes to run the streamlit application using if else.

st.set_page_config(page_title="YOUTUBE EXTRACTION",page_icon=":tada:",layout="wide")


opt = option_menu(menu_title="Menu",
                    options=['Home',"Extract and transform",'Q/A'],
                    icons=["house-fill","file-arrow-up-fill","question-square-fill"],
                    menu_icon="cast",
                    default_index=1,
                    orientation="horizontal")
            
if opt=="Home":
        st.title(''':black[YOUTUBE DATA HARVESTING AND WAREHOUSING USING YOUTUBE_API AND SQL ]''')
        st.write(" 1.  Obtaining Access to YouTube Data: You can access YouTube data through the YouTube Data API. You'll need to register your application with Google and obtain an API key. This key will allow you to make requests to the YouTube API.")
        st.write(" 2.  Decide on the Data to Harvest: Determine what specific data you want to harvest from YouTube. This could include video metadata (title, description, tags, etc.), channel information, comments, likes/dislikes, view counts, etc. ")
        st.write(" 3.  Setting Up a Database: You'll need a database system to store the harvested data. Popular choices include MySQL, PostgreSQL, or SQLite. Set up your database schema to match the data you'll be collecting.")
        st.write(" 4.  Creating Tables: In your database, create tables to store the different types of data you'll be collecting from YouTube. For example, you might have tables for videos, channels, comments, etc. Ensure proper normalization and indexing for efficient querying.")
        st.write(" 5.  Fetching Data from YouTube API: Write scripts or programs to interact with the YouTube Data API using your API key. You'll use API endpoints to retrieve the data you're interested in. Make sure to handle pagination if you're dealing with large datasets.")
        st.write(" 6.  Data Transformation and Loading (ETL): Once you retrieve data from the API, you'll need to transform it into a format suitable for storage in your database. This may involve parsing JSON responses and mapping the data to your database schema. Then, load the transformed data into your database tables.")
        st.write(" 7.  Periodic Updates: Depending on your requirements, you may want to regularly update your database with new YouTube data. Set up a process to periodically fetch updated data from the API and update your database accordingly.")
        st.write(" 8.  Handling API Quotas and Limits: Be mindful of the quotas and usage limits imposed by the YouTube API. Ensure that your harvesting process adheres to these limits to avoid being rate-limited or blocked.")
        st.write(" 9.  Data Analysis and Reporting: Once you have accumulated a significant amount of data in your warehouse, you can perform various analyses and generate reports using SQL queries. This could include trend analysis, sentiment analysis, user engagement metrics, etc.")
        st.write(" 10. Security and Compliance: Ensure that you handle YouTube data responsibly and in compliance with any relevant laws and regulations, such as data protection regulations (e.g., GDPR) and YouTube API terms of service.")
        st.subheader("Links fot the credentials")
        st.write(" 1. How to use streamlit=[Streamlit>](https://docs.streamlit.io/develop/api-reference)")
        st.write(" 2. Icons for menus=[Icons image>](https://icons.getbootstrap.com/)")
        st.write(" 3. For SQL=[SQL>](https://www.microsoft.com/en-IN/sql-server/sql-server-downloads)")
        st.write(" 4. For google developer console=[GDC>])(https://console.cloud.google.com/cloud-resource-manager)")
    




                
        
if opt == "Extract and transform":
                    
        st.markdown("#    ")
        st.write("### ENTER THE YOUTUBE CHANNEL ID ")
        channel_id = st.text_input("enter here below")
        
        if st.button('Fetch & migrate'):
                details = channel_info(channel_id)
                st.subheader('Channel Data')
                st.write(details["channel_details"])

                st.subheader('Video Data')
                st.write(details["video_details"])

                st.subheader('Comment Data')
                st.write(details["comment_details"])

if opt == "Q/A":
            questions=st.selectbox("please select",
                                            ["Choose your Questions...",
                                            '1.What are the names of the all videos and their corresponding channels?',
                                            '2. Which channels have the most number of videos, and how many videos do they have?',
                                            '3. What are the top 10 most viewed videos and their respective channels?',
                                            '4. How many comments were made on each video, and what are their corresponding video names?',
                                            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                            '8. What are the names of all the channels that have published videos in the year 2022?',
                                            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                            '10. Which videos have the highest number of comments, and what are their corresponding channel names?' ],
                                            index=0)

            if questions == '1.What are the names of the all videos and their corresponding channels?':
                        cursor.execute("""SELECT Title as Title , Channel_Name as Channel_Name  FROM youtube.Videos_info ;""")
                        df = pd.DataFrame(cursor.fetchall(), columns=['Title','Channel_Name'])
                        st.write(df)
            
            elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
                    cursor.execute("""SELECT Channel_Name, COUNT(*) AS Video_Count FROM youtube.Videos_info GROUP BY Channel_Name ORDER BY Video_Count DESC;""")
                    df = pd.DataFrame(cursor.fetchall(), columns=['Channel_Name', 'Video_Count'])
                    st.write(df)

                    
            elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
                    cursor.execute("""SELECT Title, Channel_Name, Views FROM youtube.Videos_info ORDER BY Views DESC LIMIT 10""")
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Title', 'Channel_Name', 'Views'])
                    st.write(df)
                    


            elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                    cursor.execute("""select Channel_Name , Title ,Video_id, Comments from youtube.Videos_info ;""")
                    df = pd.DataFrame(cursor.fetchall(), columns=['Channel_Name', 'Title','Video_id','Comments'])
                    st.write(df)

                    
            elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                    cursor.execute("""SELECT Channel_Name, Video_Id, Title, Likes FROM youtube.Videos_info v WHERE Likes = (SELECT MAX(Likes)
                                            FROM youtube.Videos_info WHERE Channel_Name = v.Channel_Name )""")                                
                    data = cursor.fetchall()
                    df = pd.DataFrame(data, columns=['Channel_Name','Video_Id', 'Title', 'Likes'])
                    st.write(df)
                    


            elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                        cursor.execute("""SELECT Title as Title , SUM(Likes) as Likes FROM youtube.Videos_info  GROUP BY Title""")
                        df = pd.DataFrame(cursor.fetchall(),columns=['Title', 'Likes'])
                        st.write(df)


            elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                        cursor.execute("""SELECT channel_name AS Channel_Name, View_Count AS Views FROM youtube.channel_info ORDER BY Views DESC""")
                        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                        st.write(df)

            elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                    cursor.execute("""SELECT Channel_Name AS Channel_Name FROM Videos_info WHERE published_at LIKE '2022%' GROUP BY Channel_Name ORDER BY Channel_Name""")
                    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                    st.write(df)
                                


            elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                    cursor.execute("""SELECT Channel_Name AS Channel_Name,AVG(duration)/60 AS "Average_Video_Duration (mins)" FROM youtube.Videos_info
                                        GROUP BY Channel_Name ORDER BY AVG(Duration)/60 DESC""")
                    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                    st.write(df)



            elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                cursor.execute(""" SELECT Channel_Name,  Title, Comments FROM youtube.Videos_info v WHERE Comments = (
                                                SELECT MAX(Comments) FROM youtube.Videos_info WHERE Channel_Name = v.Channel_Name ); """)               
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=['Channel_Name','Title','Comments'])
                st.write(df)
