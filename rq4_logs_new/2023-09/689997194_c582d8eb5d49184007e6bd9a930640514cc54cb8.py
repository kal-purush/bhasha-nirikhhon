from src.channel import Channel
import datetime
import isodate
from googleapiclient.discovery import build

class PlayList(Channel):
    def __init__(self, playlist_id):
        self.playlist_id = playlist_id
        self.title = self.get_playlist_info()['items'][0]['snippet']['title']
        self.url = 'https://www.youtube.com/playlist?list=' + self.playlist_id

    def get_playlist_info(self):
        """Получаем словарь плейлиста с информацией"""
        request = super().get_service().playlists().list(part="snippet", id=self.playlist_id)
        response = request.execute()

        return response

    @property
    def total_duration(self):
        """возвращает объекткласса`datetime.timedelta`ссуммарной длительности плейлиста"""
        time_line = []
        self.get_play_list()

        for video in self.get_play_list():
            iso_8601_duration = video['contentDetails']['duration']
            duration = isodate.parse_duration(iso_8601_duration)
            time_line.append(duration)

        res = sum(time_line, datetime.timedelta())
        return res

    def show_best_video(self):
        """возвращает ссылку на самое популярное видео из плейлиста"""

        high_like = 0
        url_video_top = ''
        for i in self.get_play_list():
            like = i['statistics']['likeCount']
            if int(like) > int(high_like):
                high_like = like
                url_video_top = i['id']

        return f'https://youtu.be/{url_video_top}'

    def get_play_list(self):
        playlist_videos = PlayList.get_service().playlistItems().list(playlistId=self.playlist_id,
                                                                      part='contentDetails',
                                                                      maxResults=50).execute()

        video_ids: list[str] = [video['contentDetails']['videoId'] for video in playlist_videos['items']]

        video_response = PlayList.get_service().videos().list(part='contentDetails,statistics',
                                                              id=','.join(video_ids)
                                                              ).execute()
        return video_response['items']