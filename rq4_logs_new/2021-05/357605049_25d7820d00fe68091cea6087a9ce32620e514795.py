import discord
from discord.ext import commands
from discord.utils import get
from discord.ext import tasks

import config
from pymongo import MongoClient

cluster = MongoClient(config.MONGO)
user = cluster.maidb.eco

class userDB(commands.Cog):
    def __init__(self, client):
        self.client = client


    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.client.guilds:
            for member in guild.members:
                post = {
                    "_id": member.id,
                    "guild": guild.id,
                    "balance": 0,
                    "xp": 0,
                    "lvl": 0
                }

                if user.count_documents({"_id": member.id}) == 0:
                    user.insert_one(post)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        post = {
            "_id": member.id,
            "guild": ctx.guild.id,
            "balance": 0,
            "xp": 0,
            "lvl": 0
        }

        if user.count_documents({"_id": member.id}) == 0:
            user.insert_one(post)

    @commands.Cog.listener()
    async def on_member_leave(self, member):

        post = {
            "_id": member.id,
            "guild": ctx.guild.id
        }

        user.delete_one(post)
    
def setup(client):
    client.add_cog(userDB(client))