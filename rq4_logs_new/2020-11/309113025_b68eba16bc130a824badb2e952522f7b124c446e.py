import discord

client = discord.Client()
CHANNEL_ID = 740585774055161986 #Insert channel ID here
print('One moment...')

@client.event
async def on_ready():
    e = discord.Embed(title='Server information.', colour=discord.Colour.green())
    e.add_field(name="CPU infomation", value="Null")
    e.add_field(name="RAM information", value="Null")
    e.add_field(name="Network information", value="Null")
    e.set_footer(text=f"Null")
    channel = await client.fetch_channel(CHANNEL_ID)
    message = await channel.send(embed=e)
    print(f"Setup complete!. Channel ID: {CHANNEL_ID} Message ID: {message.id}")
    print("Please copy this information into the main file. You may delete this file if you wish.")

client.run('token')