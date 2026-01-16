import discord
import os
from discord.ext import commands
from dotenv import load_dotenv

#create dictionary to hold the amount of strikes
memberstrikes = {}

#create Strike Cog
class StrikeCog(commands.Cog, name='StrikeCog'):
    def __init__(self,bot):
        self.bot = bot
        self.strikechannel = os.getenv('DISCORD_STRIKE_CHANNEL')
        
    #Simple Hello Command
    @commands.command()
    async def hello(self, ctx):
        print("Command Triggered")
        await ctx.send(f"Hello, {ctx.author}")

    #Help Command
    @commands.command()
    async def embot_help(self, ctx):
        response = 'I dont know how to do anything yet.'
        await ctx.send(response)

    #Clear all the strikes.
    @commands.command()
    @commands.has_role('Bee')
    async def strikeclear(self, ctx):
        #change this to check the role
        channel = discord.utils.get(self.bot.get_all_channels(), name = 'strikes')
        strikemessage = await channel.pins()
        await strikemessage[0].edit(content = 'Toes: 1')

    #Remove strikes
    @commands.command()
    @commands.has_role('Whole')
    async def destrike(self, ctx, striketarget, numberofstrikes = 1):
        await StrikeCog.strikemod(self, striketarget, numberofstrikes * -1)

    #Add strikes
    @commands.command()
    async def strike(self, ctx, striketarget, numberofstrikes = 1):
        await StrikeCog.strikemod(self, striketarget, numberofstrikes * 1)

    #Apply the strikes
    async def strikemod(self, striketarget, numberofstrikes = 1):
        """
        Takes the target and the number of strikes, parses through the editted
        message in the strikes channel, edits the message to include the new strikes.
        striketarget: a string that includes the memeber id inside of <@! >
        """
        #get the strikes channel - needs work
        channel = discord.utils.get(self.bot.get_all_channels(), name = 'strikes')
        strikemessage = await channel.pins()
        strikecontent = (strikemessage[0].system_content).split('\n')
        print(strikecontent)

        #parse the content in the pined message
        for member in strikecontent:
            memberstrikecount = member.split(': ')
            memberstrikes[memberstrikecount[0].lower()] = int(memberstrikecount[1])

        if striketarget not in memberstrikes:
            memberstrikes[striketarget] = numberofstrikes
        else:
            memberstrikes[striketarget] += numberofstrikes

        #create the content to post in the strikes channel
        strikepost = ''
        for x in memberstrikes:
            strikepost += f'{x}: {memberstrikes[x]}\n'
        await strikemessage[0].edit(content = strikepost)
        print(memberstrikes)

    #Add strike when using a specific emote
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        if payload.emoji.name == 'IDNAWN':
            striketarget = f'<@!{payload.user_id}>'
            await StrikeCog.strikemod(self, striketarget)

    #check messages for banned words
    @commands.Cog.listener()
    async def on_message(self, message):
        bannedwords = []
        if message.content in bannedwords:
            await message.channel.send(content='You have violated the law.')
            await StrikeCog.strikemod(self, f'<@!{message.author.id}>')
        return

def setup(bot):
    bot.add_cog(StrikeCog(bot))