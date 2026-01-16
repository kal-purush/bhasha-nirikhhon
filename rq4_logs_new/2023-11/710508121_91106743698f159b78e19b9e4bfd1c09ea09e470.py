import discord
from discord.ext import commands
import pymongo


class CSBot:
    def __init__(self, mongoUri, botToken):
        self.mongoClient = pymongo.MongoClient(mongoUri)
        self.botClient = commands.Bot(command_prefix="||||||", intents=discord.Intents.all())
        self.botToken = botToken

    def run(self):
        self.botClient.run(self.botToken)

    def break_string_into_chunks(input_string, chunk_size=2000):
        chunks = []
        current_chunk = ''

        while input_string:
            if len(input_string) <= chunk_size:
                chunks.append(input_string)
                break

            last_newline_index = input_string.rfind('\n\n', 0, chunk_size)

            if last_newline_index != -1:
                current_chunk += input_string[:last_newline_index + 1]
                input_string = input_string[last_newline_index + 1:]
            else:
                current_chunk += input_string[:chunk_size]
                input_string = input_string[chunk_size:]

            chunks.append(current_chunk)
            current_chunk = ''

        return chunks