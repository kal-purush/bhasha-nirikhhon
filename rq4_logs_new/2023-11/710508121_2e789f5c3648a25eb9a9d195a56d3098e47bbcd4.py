import discord
from discord.ext import commands
from discord import app_commands
import pymongo
import os
import random
from dotenv import load_dotenv
load_dotenv()

client = pymongo.MongoClient(os.getenv('mongo_string'))
database = client.get_database("CodingChallengeBot")
collection = database.get_collection("Challenges")



client = commands.Bot(command_prefix="", intents=discord.Intents.all())

@client.event
async def on_ready():
    commands = await client.tree.sync()
    print(f'{len(commands)} command(s) synced')

langs = ['javascript', 'java', 'c#', 'python', 'typescript', 'c++']
async def lang_autocomplete(
interaction: discord.Interaction,current: str) -> list[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=lang, value=lang) for lang in langs if current.lower() in lang.lower()
    ]
@client.tree.command(name="challenge", description="Test yourself with a coding challenge")
@app_commands.autocomplete(lang=lang_autocomplete)
async def challenge(interaction: discord.Interaction, lang: str):

    if (lang not in langs):
        embed=discord.Embed(title="Whoops!", description="**That language is not supported!**", color=0xff0000)
        embed.set_footer(text="Please try again!")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    
    docs = list(collection.find({"languages": {"$in": ["python"]}}))
    doc = docs[random.randint(0, len(docs) - 1)]
    
    #await interaction.response.send_message(doc["description"], ephemeral=True)

    embed=discord.Embed(title=doc["name"], description=doc["category"], color=0x1f5ad1)
    embed.add_field(name="Code Wars Link", value=doc["url"], inline=True)
    embed.add_field(name="Difficulty", value=doc["difficulty"], inline=True)
    await interaction.response.send_message(doc["description"], embed=embed, ephemeral=True)


client.run(os.getenv('bot_token'))
