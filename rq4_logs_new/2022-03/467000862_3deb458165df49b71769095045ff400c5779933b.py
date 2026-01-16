import discord
from discord.ext import commands

bot = commands.Bot(command_prefix = "!", description = "j'aime les femmes")

@bot.command()
async def avatar(ctx, member: discord.Member):
    show_avatar = discord.Embed(

        color = discord.Color.dark_blue()
    )
    show_avatar.set_image(url='{}'.format(member.avatar_url))
    await ctx.send(embed=show_avatar)


bot.run("OTUzMDQwNjkwNzAxNTUzNzE1.Yi-yTw.na_p5Xv15Co_GXYvhzKwpVVl-LI")