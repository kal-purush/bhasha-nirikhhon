import discord
from discord.ext import commands
import json
from datetime import datetime as date

client = discord.Client()
bot = commands.Bot(command_prefix = "|")



routine_file = open("./assets/data.json",)
data = json.load(routine_file)




############################EVENTS################################
@bot.event
async def on_ready():
    print("Hello, I am READY")
    await bot.change_presence(activity = discord.Activity(type = discord.ActivityType.listening , name = "|help"))







@bot.event
async def on_message(message):
    if message.content == "|Hello":
        await message.channel.send("Hello {}".format(message.author.mention))

    await bot.process_commands(message)

    # if not hasattr(client , "appInfo"):
    #     client.appInfo = await client.application_info()









@bot.event
async def on_command_error(ctx , error):
    if isinstance(error , commands.CommandNotFound):
        await ctx.send("Invalid Command.")

############################EVENTS END################################










############################COMMANDS##############################

@bot.command(aliases = ['clear' , 'Purge'])
async def purge(ctx,amount = 9):
    await ctx.channel.purge(limit = amount+1)







@bot.command(aliases =['Routine', 'routine'])
async def fetch(ctx):
    embed = discord.Embed(
        title = "Links for classes",
        description = "Hello, I will help you find all the links for your classes."
    )
    await ctx.channel.send(embed = embed)

    count = 1
    section = ""
    subject = ""
    day = date.today().strftime("%A")

    while(count<=3):

        if count == 1:
            await ctx.channel.send("```Give me your Section```")
            section = await bot.wait_for("message" , check = lambda message : ctx.author == message.author)
            if section:
                count = count + 1
                continue
        
        if count == 2:
            await ctx.channel.send("```Which subject link do you want? Add '_t' or '_l' for tutorial and labs respectively at the end of subject name```")
            subject = await bot.wait_for("message" , check = lambda message : ctx.author == message.author)
            if subject:
                count = count + 1
                continue

        if count == 3:
            await ctx.channel.send("```Tell me the day for which you want.(Today is {})```".format(day))
            day = await bot.wait_for("message" , check = lambda message : ctx.author == message.author)
            if day:
                count = count + 1
                continue



    section = section.content.lower()
    subject = subject.content.lower()
    day = day.content.lower()



    try:
        await ctx.channel.send("```Here's your link :``` {}".format(data[section][day][subject]))
    except:
        await ctx.channel.send("```Can't find the your link. You might have made a typo, or maybe I have to update myself```")
            
    await ctx.channel.send("{} {} {}".format(section.content , subject.content , day.content))













@bot.command(aliases=["All" , "all" , "showAll", "show"])
async def showall(ctx, section):

    await ctx.channel.send("Here is your Routine for Section {}".format(section))
    section = section.lower()
    message = ""

    for sec in data[section]:
        message = message + sec+ " :\n"
        for sub in data[section][sec]:
            message = message + sub + "  "
        message = message+ "\n\n"

    
    await ctx.channel.send("```{}```".format(message))





############################COMMANDS END##############################











f = open("./token/token.json" ,)
s = json.load(f)



bot.run(s["token"])