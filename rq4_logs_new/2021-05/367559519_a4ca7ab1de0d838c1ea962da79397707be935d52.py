from asyncio import sleep
from discord import Embed, TextChannel, User
from discord.ext import commands

class Messages(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @commands.command(aliases=['remove', 'purge'])
    @commands.has_permissions(manage_messages=True)
    async def delete(self, ctx, limit: int, target: User = None):
        """Remove the specified amount of messages"""
        await ctx.message.delete()
        if target is None:
            await ctx.message.channel.purge(limit=limit)
        else:
            await ctx.messsage.channel.purge(
                limit=limit, check=lambda message: message.author == target)
        
    @delete.error
    async def purge_error(self, ctx, error):
        if isinstance(error, commands.MissingPermissions):
            await ctx.message.delete()
            message = await ctx.send(
                "You are missing the 'Manage Messages' permission."
            )
            await sleep(3)
            await message.delete()

def setup(bot):
    bot.add_cog(Messages(bot))