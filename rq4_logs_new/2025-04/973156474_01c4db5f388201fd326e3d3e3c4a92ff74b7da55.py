from moviepy import AudioFileClip
import os, argparse
import logging
import shutil

logger = logging.getLogger(__name__)
logging.basicConfig(filename='./data/converter_audio.log', encoding='utf-8', level=logging.DEBUG)

class ConverterToAudio:
    def __init__(self, video_media_path, audio_media_path, ouput_media_path = './data/media'):
        self.video_media_path = video_media_path
        self.audio_media_path = audio_media_path
        self.ouput_media_path = ouput_media_path
        
    def convert(self):
        if self.audio_media_path is not None:
            # input_audio_path = args.audio_media_path
            # Existe un path de entrada de audio ??
            pass
        else:
            os.mkdir('audio', exist_ok=True)
            self.audio_media_path = os.path.join(os.getcwd(), 'audio')       

        if self.video_media_path is not None:
            #input_path = args.video_media_path
            # Existe un path de entrada de video ??            
            if not os.path.exists(self.video_media_path):
                logger.warning(f"Path de entrada: {self.video_media_path} con videos para convertir, no existe.")
                #exit(1)
            video_files = [video for video in os.listdir(self.video_media_path) if video.endswith(".mp4")]
            # Convert the video to audio
            for video_file in video_files:
                audio_file = video_file.replace('.mp4', '.wav')
                self.convertVideoToAudio(os.path.join(self.video_media_path, video_file), os.path.join(self.audio_media_path, audio_file))
                logger.info(f"Converted {video_file} to {audio_file}.")
        for audio_file in os.listdir(self.audio_media_path):
            if audio_file.endswith(".wav"):
                # Move the audio files converted or not converted to the output path 
                shutil.copy2(os.path.join(self.audio_media_path, audio_file), os.path.join(self.ouput_media_path, audio_file))                   
                #exit(1)
        
     # Convert MP4 video to WAV audio   
    def convertVideoToAudio(self, input_file, output_path):
        # Load the MP4 audio file
        audio = AudioFileClip(input_file)
        # Convert and save the audio to WAV format
        audio.write_audiofile(output_path, codec='pcm_s16le', bitrate='16000')
        audio.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert MP4 video to WAV audio")
    parser.add_argument('-vmp', '--video_media_path', type=str, default='../datasets', help='Path of the folder with the video(.mp4) files')
    parser.add_argument('-amp', '--audio_media_path', type=str, default='../datasets', help='Path of the folder with the audio(.wav) files')

    args = parser.parse_args()
    converter = ConverterToAudio(args.video_media_path, args.audio_media_path)
    converter.convert()
    # Convert the video to audio