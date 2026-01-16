from pytube import YouTube
import re
from FrameExtractor import FrameExtractor

class DownloadAndExtract():
    def __init__(self, video_url):
        video = YouTube(video_url)

        self.video_url = video_url
        self.video = video
        self.charTitle = re.sub('[\W_]+', '', video.title)
        self.isDownloaded = False
    
    def printBasicInfo(self):
        video = self.video
        print('Summary:')
        print(f'Title: {video.title}')
        print(f'Duration: {video.length / 60:.2f} minutes')
        print(f'Rating: {video.rating:.2f}')
        print(f'# of views: {video.views}')

    def download(self):
        video = self.video
        charTitle= self.charTitle

        video.streams.all()
        video.streams.filter(file_extension = "mp4").all()
        video.streams.get_by_itag(18).download(filename=charTitle)
        self.isDownloaded = True

    def extractFrames(self, perFrame = 1000):
        charTitle= self.charTitle

        if not self.isDownloaded:
            self.download()


        fe = FrameExtractor('./'+charTitle+'.mp4')
        fe.n_frames
        fe.get_video_duration()
        fe.get_n_images(every_x_frame=perFrame)

        # extract every 1000th frame
        fe.extract_frames(every_x_frame=perFrame, 
                        img_name=charTitle, 
                        dest_path='frames/'+charTitle+'_images')


