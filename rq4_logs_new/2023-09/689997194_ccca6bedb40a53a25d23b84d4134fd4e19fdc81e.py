from src.channel import Channel
from googleapiclient.discovery import build
import os
class Video(Channel):
    api_key: str = os.getenv('YT_API_KEY')
    __youtube = build('youtube', 'v3', developerKey=api_key)

    def __init__(self, video_id):
        self.video_id = video_id
        self.video_response = self.__youtube.videos().list(part='snippet,statistics,contentDetails,topicDetails',
                                                 id=self.video_id
                                                 ).execute()

        self.video_title = self.video_response['items'][0]['snippet']['title']
        self.video_url = f'https://www.youtube.com/video/{self.video_id}'
        self.views_count = self.video_response['items'][0]['statistics']['viewCount']
        self.like_count = self.video_response['items'][0]['statistics']['likeCount']
        self.duration = self.video_response['items'][0]['contentDetails']['duration']

    def __str__(self):
        return f"{self.video_title}"


class PLVideo(Video):

    def __init__(self, video_id, playlist_id):
        super().__init__(video_id)
        self.playlist_id = playlist_id