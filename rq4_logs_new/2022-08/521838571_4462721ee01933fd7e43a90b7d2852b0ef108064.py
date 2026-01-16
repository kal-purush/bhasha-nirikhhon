## Initialisation
import boilerBot.lib as lib
import discord

from discord.ext import commands, tasks
from common import Player

## Define command cog
class nowplaying(commands.Cog):
    ## Initialise with help info
    def __init__(self,bot):
        self.bot = bot
        self.category = lib.getCategory(self.__module__)
        self.description = "Shows what the bot is now playing"
        self.usage = f"""
        {self.bot.command_prefix}nowplaying
        """
        self.hidden = False
        
    @commands.command()
    async def nowplaying(self, ctx, *command):
        guildVars = lib.retrieve(ctx.guild.id, self.bot)
        # Check bot is not already playing music, and if it is, check caller is allowed to move users
        if not guildVars['player']:
            embed = lib.embed(
                title = "Nothing is currently playing",
                colour = lib.errorColour
            )
        else:
            embed = guildVars['player'].nowPlaying()
        guildVars["previous"] = await lib.send(ctx,embed,guildVars["previous"])
        lib.set(ctx.guild.id,self.bot,guildVars)
        return

def setup(bot):
    bot.add_cog(nowplaying(bot))