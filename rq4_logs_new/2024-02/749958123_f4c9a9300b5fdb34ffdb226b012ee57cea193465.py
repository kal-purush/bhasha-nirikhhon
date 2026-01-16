from googleapiclient.discovery import build
import os
import json
import isodate
from datetime import timedelta


api_key: str = os.getenv('YT_API_KEY')
youtube = build('youtube', 'v3', developerKey=api_key)


class PlayList:
    youtube = build('youtube', 'v3', developerKey=api_key)

    def __init__(self, playlist_id: str):
        self.playlist_id = playlist_id
        self.playlists = self.get_service().playlists().list(id=self.playlist_id, part='contentDetails,snippet',
                                                             maxResults=50,).execute()
        self.url = f'https://www.youtube.com/playlist?list={playlist_id}'
        self.title = self.playlists['items'][0]['snippet']['title']
        self.playlist_videos = youtube.playlistItems().list(playlistId=self.playlist_id,
                                                            part='contentDetails', maxResults=50,).execute()
        video_ids: list[str] = [video['contentDetails']['videoId'] for video in self.playlist_videos['items']]
        self.video_response = youtube.videos().list(part='contentDetails,statistics', id=','.join(video_ids)).execute()

    @property
    def total_duration(self):
        total_duration = timedelta(days=0,
                                   seconds=0,
                                   microseconds=0,
                                   milliseconds=0,
                                   minutes=0,
                                   hours=0,
                                   weeks=0)
        for video in self.video_response['items']:
            # YouTube video duration is in ISO 8601 format
            iso_8601_duration = video['contentDetails']['duration']
            duration = isodate.parse_duration(iso_8601_duration)
            total_duration += duration
        return total_duration

    def show_best_video(self):
        playlist_videos = youtube.playlistItems().list(playlistId=self.playlist_id,
                                                       part='contentDetails',
                                                       maxResults=50,
                                                       ).execute()
        video_ids: list[str] = [video['contentDetails']['videoId'] for video in playlist_videos['items']]
        max_like = 0
        max_video = ''
        for id_v in video_ids:
            video_response = youtube.videos().list(part='snippet,statistics,contentDetails,topicDetails',
                                                   id=id_v).execute()
            like_count: int = video_response['items'][0]['statistics']['likeCount']
            if int(like_count) > int(max_like):
                max_video = video_response
            else:
                continue
        return f'https://youtu.be/{max_video['items'][0]['id']}'

    def print_info(self) -> None:
        """Выводит в консоль информацию о канале."""
        print(json.dumps(self.playlists, indent=2, ensure_ascii=False))

    @classmethod
    def get_service(cls):
        return cls.youtube