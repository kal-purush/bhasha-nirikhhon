# STANDARD LIB
import logging

# EXTERNAL LIB
import discord
import requests
import youtube_dl

# LOCAL LIB
from ._const import *

def link_valid(link):
    try:
        request = requests.head(link)
        code = request.status_code
        if (code > 199 or code < 400):
            logging.info(f'Link resolved successfully [Code {code}]')
            return True
        else:
            logging.info(f'Link received error [Code {code}]')
            return False
    except Exception:
        logging.info('Error resolving link!')
        logging.debug('\n', exc_info=True)
        return False

class MusicModule():
    def __init__(self, parent):
        self.parent = parent
        self.ytdl = youtube_dl.YoutubeDL(YTDL_OPTIONS)
    
    async def play(self, ctx, url):
        if not self.parent.is_connected(ctx.author.guild):
            if not ctx.author.voice:
                await ctx.send('You must be in a voice channel to use this command.')
                return
            await self.parent.join(ctx.author.voice.channel)
        
        if not link_valid(url.strip()):
            await ctx.send('I couldn\'t read that URL.')
            return
        
        # await ctx.defer()
        filename = await Download.from_url(url, loop=self.parent.loop)
        stream = discord.FFmpegPCMAudio(executable=FFMPEG_PATH, source=filename)
        
        try:
            ctx.author.guild.voice_client.play(stream)
            await ctx.send(f'**Now playing:** {filename}')
        except discord.errors.ClientException:
            # TODO: Add queue
            await ctx.send('Music is already playing. A queue will be implemented soon!')

    async def stop(self, ctx):
        if not self.parent.is_connected(ctx.author.guild):
            await ctx.send('I\'m not connected to any voice channels!')
        else:
            await self.parent.close_connection(ctx.author.guild)
            await ctx.send('Successfully disconnected.')

class Download(discord.PCMVolumeTransformer):
    def __init__(self, source, *, data, volume=0.5):
        super().__init__(source, volume)
        self.data = data
        self.title = data.get('title')
        self.url = ""

    @classmethod
    async def from_url(cls, url, *, loop=None, stream=False):
        ytdl = youtube_dl.YoutubeDL(YTDL_OPTIONS)
        loop = loop or asyncio.get_event_loop()
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(url, download=not stream))
        if 'entries' in data:
            # take first item from a playlist
            data = data['entries'][0]
        filename = data['title'] if stream else ytdl.prepare_filename(data)
        return filename