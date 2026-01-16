from googleapiclient.discovery import build
import os
import json

api_key: str = os.getenv('YT_API_KEY')
youtube = build('youtube', 'v3', developerKey=api_key)


class Video:
    def __init__(self, video_id: str):
        self.video_id = youtube.videos().list(part='snippet,statistics,contentDetails,topicDetails',
                                              id=video_id).execute()
        self.id = self.video_id['items'][0]['id']
        self.title = self.video_id['items'][0]['snippet']['title']
        self.url = f'https://youtube.com/watch/{self.video_id['items'][0]['id']}'
        self.view_count = self.video_id['items'][0]['statistics']["viewCount"]
        self.like_count = self.video_id['items'][0]['statistics']["likeCount"]

    def __str__(self):
        return f'{self.title}'

    def print_info(self) -> None:
        """Выводит в консоль информацию о канале."""
        print(json.dumps(self.video_id, indent=2, ensure_ascii=False))


class PLVideo:

    def __init__(self, video_id: str, playlist_id: str):
        self.video_id = youtube.videos().list(part='snippet,statistics,contentDetails,topicDetails',
                                              id=video_id).execute()
        self.playlist_id = youtube.playlistItems().list(playlistId=playlist_id,
                                                        part='contentDetails', maxResults=50,).execute()
        self.id = self.video_id['items'][0]['id']
        self.title = self.video_id['items'][0]['snippet']['title']
        self.url = f'https://youtube.com/watch/{self.video_id['items'][0]['id']}'
        self.view_count = self.video_id['items'][0]['statistics']["viewCount"]
        self.like_count = self.video_id['items'][0]['statistics']["likeCount"]
        self.id_pls = self.playlist_id['items'][0]['id']

    def __str__(self):
        return f'{self.title}'

    def print_info(self) -> None:
        """Выводит в консоль информацию о канале."""
        print(json.dumps(self.playlist_id, indent=2, ensure_ascii=False))