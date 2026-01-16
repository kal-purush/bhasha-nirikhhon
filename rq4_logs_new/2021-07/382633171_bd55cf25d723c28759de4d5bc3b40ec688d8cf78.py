import discord
from discord.ext import commands

import asyncio
import random

import os
import sys


class Mods(commands.Cog):

    def __init__(self, client):
        self.client = client

    # # FUTURE ALGORITHM
    # @commands.command(name="MODSEARCHCOMMAND", aliases=['mod'])
    # async def _modsearch(self, ctx, arg):
    #
    #     # Func to search for <arg> on penmodding.pm
    #         top_result = {first_result}
    #
    #     try # Search func:
    #         await ctx.send(f'{arg}: {top_result})
    #     except # Results not found:
    #         await ctx.send('``! Please input a valid mod name !``')



def setup(client):
    client.add_cog(Mods(client))