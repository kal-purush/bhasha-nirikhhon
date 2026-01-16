import discord
from discord.ext import commands, tasks
from discord.voice_client import VoiceClient
import datetime
import utils
import sys
#import mongo
#import db
from pathlib import Path
from googletrans import LANGUAGES
from core_file.kolbaser_methods import *
from core_file.kolbaser_request import *
#from core_file.datab import *
import asyncio
from typing import Union
import cmath
import itertools
import platform
import logging
import motor.motor_asyncio

intents = discord.Intents.default()
intents.members = True
intents.presences = True
client = commands.Bot(command_prefix='k ', intents=intents)
id = client.get_guild(681262466650734596)
ROLE = "1 poloski"
PATH = r"C:\Users\Addy\Desktop\F Files\Nocosteyhed\Locker\Private\F8\HYDROFLOURIC\Kolbaser[5] 2021"
UserDebil = "Debil! You need to specify a user"
oBlyat = "Debil! Suggest what?"
oDebil = "Say what?"
HF = 273255595509809162
Kolbaser = 690305523903758612
stats2 = itertools.cycle(["Oi Blyat!", "Final Development 5.9.3", "on {x} Servers",  "with {y} users"])

yescyecyebez = ["cogs.RPS", "cogs.Donate", "cogs.tpb", "cogs.mute", "cogs.infocmd", "cogs.Ping", "cogs.Bakiyevez", "cogs.Moderation"]

if __name__ == '__main__':
    for extension in yescyecyebez:
        client.load_extension(extension)
        #print(extension)
        b = extension
        #print(f"{b[5:]} extension loaded")
        #print(f"The {b[5:]} extension was loaded")
        print("Extensions loaded")

polska = Path(__file__).parents[0]
serb = str(polska)
print(f"{serb}")
logging.basicConfig(level=logging.INFO)
client.black_ppl = []
#----------------------------------SHUT DOWN
@client.command()
@commands.is_owner()
async def idinahui(ctx):
    try:
        await ctx.send("Bye Blyat")
        await client.close()
        time.sleep(5)
    except discord.ext.commands.errors.NotOwner:
        await ctx.send("Cyka")
        return


################################################################################
# KOLBASER EVENTS

@client.event
async def on_ready():
    stat.start()
    print("Loop Started")
    #blacks = read_json("BL") #data
    #client.black_ppl = blacks["negros"]
    #client.mongo
    print("Bot is ready, blyat")


@tasks.loop(minutes=1)
async def stat():
    n = next(stats2)
    n2 = n.format(x=len(client.guilds), y=sum([len(guild.members) for guild in client.guilds]))
    await client.change_presence(activity=discord.Game(name=n2))
    return


@client.event
async def on_message(message):
    if message.author == client.user:
        return



    else:
        msg = message_events(message.content.lower())
        if msg:
            await message.channel.send(f'{msg} {message.author.mention}')

        await client.process_commands(message)


@client.event
async def on_message_edit(before, after):
    if before.author == client.user:
        return
    try:
        channel = secret_get_channel(before.guild.id)
    except NeedInstallFirst:
        print('Message edit need to install! <install_spy>')
        return

    if channel is not None:
        ch = client.get_channel(channel)

        if len(before.embeds) != 0:
            pass


        else:
            embed = discord.Embed(
                title='Message Edit Log',
                description=f'`{str(before.author)}` edited a message on Server: ',
                colour=0xdd16d9
                )
            embed.add_field(name='Server :',value=f'`{str(before.guild)}`', inline=False)
            embed.add_field(name='Channel :',value=f'`{str(before.channel)}`', inline=False)
            embed.add_field(name='Message Before :',value=f'`{before.content}`')
            embed.add_field(name='Message After :',value=f'`{after.content}`')
            embed.set_footer( text=f'Time of Edit : {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)


@client.event
async def on_message_delete(message):
    try:
        channel = secret_get_channel(message.guild.id)
        print(channel)
    except NeedInstallFirst:
        print('Message delete need to install! <install_spy>')
        return

    if message.author == client.user:
        return

    if channel is not None:
        ch = client.get_channel(channel)
        print(ch)

        if len(message.embeds) != 0:
            return

        if message.attachments:
            embed=discord.Embed(
                title='Deleted Messaged Log',
                description=f'`{message.author}` deleted a message',
                colour=0xdd16d9
            )
            embed.add_field(name='Attachment :',value=f'{message.attachments[0].url}', inline=False)
            embed.add_field(name='Server:',value=f'`{message.guild}`', inline=False)
            embed.add_field(name='Channel:',value=f'`{message.channel}`', inline=False)
            embed.set_footer(text=f'Time: {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)
        else:
            embed = discord.Embed(
                title='Deleted Messaged Log',
                description=f'`{message.author}` deleted a message',
                colour=0xdd16d9
                )
            embed.add_field(name='Server :',value=f'`{message.guild}`', inline=False)
            embed.add_field(name='Channel :',value=f'`{message.channel}`', inline=False)
            embed.add_field(name='Message :',value=f'`{message.content}`', inline=False)
            await ch.send(embed=embed)


@client.event
async def on_reaction_remove(reaction, user):
    if reaction.message.author == client.user:
        return
    try:
        channel = secret_get_channel(reaction.message.guild.id)
    except NeedInstallFirst:
        print('reaction edit need to install! <install_spy>')
        return
    if channel is not None:

        ch = client.get_channel(channel)

        embed = discord.Embed(title='Reaction-Remove Log',description=f'`{user.display_name}` Removed a Reaction', colour=0xdd16d9)
        embed.add_field(name='Server :',value=f'`{reaction.message.guild}`', inline=False)
        embed.add_field(name='Channel :',value=f'`{reaction.message.channel}`', inline=False)
        embed.add_field(name='Emoji :',value=f'{reaction}', inline=False)
        embed.add_field(name='Message', value=f'`{reaction.message.content}`', inline=False)
        await ch.send(embed=embed)

        if reaction.message.attachments:
            embed.add_field(name='Attachment :',value=f'{reaction.message.attachments.url[0]}', inline=False)
            embed.set_footer(text=f'Time: {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)


@client.event
async def on_reaction_add(reaction, user):
    if reaction.message.author == client.user:
        return
    try:
        channel = secret_get_channel(reaction.message.guild.id)
    except NeedInstallFirst:
        print('reaction edit need to install! <install_spy>')
        return

    if channel is not None:

        ch = client.get_channel(channel)

        embed = discord.Embed(title='Reaction-Add Log',description=f'`{user.display_name}` Added a Reaction', colour=0xdd16d9)
        embed.add_field(name='Server :',value=f'`{reaction.message.guild}`', inline=False)
        embed.add_field(name='Channel :',value=f'`{reaction.message.channel}`', inline=False)
        embed.add_field(name='Emoji :',value=f'{reaction}', inline=False)
        embed.add_field(name='Message', value=f'`{reaction.message.content}`', inline=False)
        await ch.send(embed=embed)

        if reaction.message.attachments:
            embed.add_field(name='Attachment :',value=f'{reaction.message.attachments.url}', inline=False)
            embed.set_footer(text=f'Time: {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)


@client.event
async def on_member_update(before, after):

    def check_roles() -> Union[Tuple[str, dict], None]:

        before_roles = [role.name for role in before.roles]
        after_roles = [role.name for role in after.roles]

        removed_roles = set(before_roles) - set(after_roles)
        upgrade_roles = set(after_roles) - set(before_roles)

        if upgrade_roles:
            return 'Upgrade', ''.join(list(upgrade_roles))
        elif removed_roles:
            return 'Removed', ''.join(list(removed_roles))
        else:
            return None

    def check_nick() -> bool:
        if before.nick != after.nick:
            return True
        else:
            return False

    def check_avatar():
        if before.avatar != after.avatar:
            return True
        else:
            return False
    try:
        channel = update_get_channel(before.guild.id)
    except NeedInstallFirst:
        print('update member command has to install <install_updates>')
        return

    if before.nick == client.user:
        return

    if channel is not None:
        ch = client.get_channel(channel)
        if check_roles() or check_nick() or check_avatar():
            embed = discord.Embed(
                title='Users Updates', description=f'`{before.display_name}` Changed his profile', colour=0xdd16d9)

        if check_roles() is not None:
            embed.add_field(name=f'`{check_roles()[0]}` role', value=f'`{check_roles()[1]}`', inline=False)
            embed.set_footer(text=f'Time : {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)
        if check_nick():
            embed.add_field(name='Before name', value=f'from `{before.nick}` ')
            embed.add_field(name='After name', value=f'To `{after.nick}`')
            embed.set_footer(text=f'Time : {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)

        if check_avatar():
            embed.add_field(name='Avatar Before', value=f'from {before.avatar}')
            embed.add_field(name='After Avatar', value=f'To {after.avatar}')
            embed.set_footer(text=f'Time : {datetime.now().strftime("%H:%M:%S")}')
            await ch.send(embed=embed)


@client.event
async def on_member_join(member):
    try:
        channel = welcome_get_channel(member.guild.id)
    except NeedInstallFirst:
        print('on member must install by <install_welcome>')
        return

    if channel is not None:
        ch = client.get_channel(channel)
        embed = discord.Embed(title='**Member Joined!**',description=f'{member}', colour=0xdd16d9)
        embed.add_field(name=f'Welcome to {member.guild}', value='Do `k kolbaser` for information', inline=False)
        embed.add_field(name='Account Creation Date:',value=f'{member.created_at.strftime("%A, %B %#d, %Y, %H:%M UTC")}', inline=False)
        embed.set_thumbnail(url=member.avatar_url)
        embed.set_footer(text=f'Time of join: {datetime.now().strftime("%H:%M:%S")}')
        await ch.send(embed=embed)

        role = discord.utils.get(member.guild.roles, name='1 poloski')
        if role in member.guild.roles:
            await member.add_roles(role)
            print("noob")
            try:
                await member.send(f'Привет {member.name}, Thank you for joining! ')
            except discord.errors.Forbidden:
                pass
        else:
            pass


@client.event
async def on_member_remove(member):
    try:
        channel = goodbye_get_channel(member.guild.id)
    except NeedInstallFirst:
        print('remove member commad has to install <install_goodbye>')
        return

    if channel is not None:
        ch = client.get_channel(channel)
        embed = discord.Embed(title='**Member Left!**',description=f'{member}', colour=0xdd16d9)
        embed.add_field(name=f'{member} left ***{member.guild}***', value='До свидания', inline=False)
        embed.set_thumbnail(url=member.avatar_url)
        embed.set_footer(text=f'Time of Leave: {datetime.now().strftime("%H:%M:%S")}')
        await ch.send(embed=embed)


################################################################################
# KOLBASER SPY COMMANDS

@client.command()
@commands.has_permissions(administrator=True)
async def install_spy(ctx):
    update_secret(ctx.guild.id, ctx.channel.id)
    await ctx.send('Spy events installed successfully!')


@client.command()
@commands.has_permissions(administrator=True)
async def uninstall_spy(ctx):
    if uninstall_secret(ctx.guild.id):

        await ctx.send('Spy events uninstalled successfully!')
    else:
        await ctx.send('something went wrong .. check the file')


@client.command()
@commands.has_permissions(administrator=True)
async def install_welcome(ctx):
    update_welcome(ctx.guild.id, ctx.channel.id)
    await ctx.send('Welcome Events Installed successfully')


@client.command()
@commands.has_permissions(administrator=True)
async def uninstall_welcome(ctx):
    if uninstall_welcome(ctx.guild.id):
        await ctx.send('Spy events uninstalled successfully!')
    else:
        await ctx.send('something went wrong .. check the file')


@client.command()
@commands.has_permissions(administrator=True)
async def install_goodbye(ctx):
    channel = update_goodbye(ctx.guild.id, ctx.channel.id)
    await ctx.send('Goodbye Events Installed successfully')


@client.command()
@commands.has_permissions(administrator=True)
async def uninstall_goodbye(ctx):
    if uninstall_goodbye(ctx.guild.id):

        await ctx.send('Goodbye events uninstalled successfully!')
    else:
        await ctx.send('something went wrong .. check the file')


@client.command()
@commands.has_permissions(administrator=True)
async def install_updates(ctx):
    channel = update_update(ctx.guild.id, ctx.channel.id)
    await ctx.send('Update Events Installed successfully')


@client.command()
@commands.has_permissions(administrator=True)
async def uninstall_updates(ctx):
    check_uninstall = uninstall_update(ctx.guild.id)
    if check_uninstall:
        await ctx.send('Updates events uninstalled successfully!')
    else:
        await ctx.send('something went wrong .. check the file')


###############################################################################
# KOLBASER ADMIN COMMANDS

@client.command()
@commands.has_permissions(manage_messages=True)
async def clear(ctx, number: int):
    await ctx.channel.purge(limit=number)


@client.command()
@commands.has_permissions(administrator=True)
async def gulag(ctx, member: discord.Member, reason = None):
    if member.id == HF:
        await ctx.send("I will not send my Lord and Savior to the gulag!")
        return
    if member.id == Kolbaser:
        await ctx.send("Why in the blyat would I send myself to the gulag?")
        return
    else:
        def get_prisoner_role():
            prisoner_role = discord.utils.get(ctx.guild.roles, name='gulag prisoner')
            return prisoner_role

        def get_prisoner_channel():
            prisoner_channel = discord.utils.get(ctx.guild.text_channels, name='gulag')
            return prisoner_channel

        # create prisoner ROLE

        if get_prisoner_role() is None:
            await ctx.guild.create_role(name='gulag prisoner', color=discord.Colour(0x090909))

        # create prisone CHANNEL

        if get_prisoner_channel() is None:
            await ctx.guild.create_text_channel(name='gulag')

        # permissions for other channels -> except gulag prison
        overwrite = discord.PermissionOverwrite()
        overwrite.view_channel = False
        all_channels = [channel for channel in ctx.guild.text_channels if channel.name != 'gulag']

        for channel in all_channels:
            await channel.set_permissions(get_prisoner_role(), overwrite=overwrite)

        # permissions for everyone to gulag prison
        everyone_role = discord.utils.get(ctx.guild.roles, name='@everyone')

        for_all = discord.PermissionOverwrite()
        for_all.view_channel = False

        role_gulag = discord.PermissionOverwrite()
        role_gulag.view_channel = True

        await get_prisoner_channel().set_permissions(everyone_role, overwrite=for_all)
        await get_prisoner_channel().set_permissions(get_prisoner_role(), overwrite=role_gulag)

        # remove roles from member and giving the role gulag
        roles_member = [role for role in member.roles if role.name != '@everyone']
        for role in roles_member:
            await member.remove_roles(role)

        await member.add_roles(get_prisoner_role())

        embed = discord.Embed(
            title='Gulag Prison', description='You dont want to go there', colour=0xdd16d9)
        embed.add_field(name=f'{str(member)} Went to Gulag Prison', value=f'Reason : {reason}')
        await ctx.send(embed=embed)


@client.command()
@commands.has_permissions(administrator=True, manage_messages=True)
async def nuke(ctx):
    await ctx.channel.purge(limit=1000000)


@client.command()
@commands.has_permissions(administrator=True)
async def free(ctx, member: discord.Member):
    gulag_prison = discord.utils.get(ctx.guild.roles, name="gulag prisoner")
    await member.remove_roles(gulag_prison)
    embed = discord.Embed(
        title='Freedom', description=f'{str(member)} Is Free now')
    await ctx.send(embed=embed)


@client.command()
@commands.has_permissions(administrator=True)
async def ban(ctx, member: discord.Member):
    await member.ban()
    await ctx.send(":hammer: `{}` **was banned by** `{}`".format(member.name, ctx.author.name))
    return


@client.command()
@commands.has_permissions(administrator=True)
async def warn(ctx, member: discord.Member, *, reason=None):
    await ctx.channel.purge(limit=1)

    embed = discord.Embed(
            description=f'You have been warned in `{ctx.guild}`\n{reason}\nAt: {datetime.now().strftime("%H:%M:%S")}',
            color=0xdd16d9
        )

    dmw = await member.create_dm()
    await dmw.send(embed=embed)


@client.command()
@commands.is_owner()
async def dm(ctx, user: discord.Member, *, message=None):
    await ctx.channel.purge(limit=1)

    embed = discord.Embed(
            description=message,
            color=0xdd16d9
        )

    channel = await user.create_dm()
    await channel.send(embed=embed)
    channel = client.get_channel(699655178818814013)
    embed = discord.Embed(
            title='Direct Message Log',
            description=f'Message: `{message}`\nAuthor: `{ctx.author}`\nRecipient: `{user}`',
            colour=0xdd16d9
        )
    await channel.send(embed=embed)


###############################################################################
# KOLBASER PUBLIC COMMANDS


@client.command(aliases=["server"])
async def serverinfo(ctx):
    Memberz = MembersBlyat(ctx)
    def get_channels():
        TotalTextBlyat = len(ctx.guild.text_channels)
        return TotalTextBlyat
    def get_channels2():
        TotalVoiceBlyat = len(ctx.guild.voice_channels)
        return TotalVoiceBlyat
    def ChannelsByat():
        Blyat1 = get_channels()
        Blyat2 = get_channels2()
        TotalNahui = (Blyat1 + Blyat2)
        return TotalNahui
    embed = discord.Embed(
        colour=0xdd16d9
    )
    embed.set_author(name=f'KOLBASER\nServer Information')
    embed.set_footer(text=f'Requested by {ctx.author}', icon_url=ctx.author.avatar_url)
    embed.set_thumbnail(url=ctx.guild.icon_url)

    embed.add_field(name='Server Name:', value=f'`{ctx.guild}`', inline=False)
    embed.add_field(name='Server ID:', value=f'`{ctx.guild.id}`', inline=False)
    embed.add_field(name='Server Owner:',value=f'`{ctx.guild.owner}`', inline=False)
    embed.add_field(name='Server Creation Date:',value=f'`{ctx.guild.created_at.strftime("%A, %B %#d, %Y, %H:%M UTC")}`', inline=False)
    embed.add_field(name=f'Number of members:',value=f'`{Memberz}`', inline=False)
    embed.add_field(name=f'Number of channels:',value=f'`{ChannelsByat()}`', inline=False)
    embed.add_field(name=f'Number of roles',value=f'`{ServerRolesBlyat(ctx)}`', inline=False)
    await ctx.send(embed=embed)


@client.command(aliases=["user"])
async def userinfo(ctx, member: discord.Member = None):
    if not member:
        await ctx.send(UserDebil)
    else:
        embed = discord.Embed(
            colour=0xdd16d9
        )
        embed.set_author(name='KOLBASER\nUser Information')
        embed.set_thumbnail(url=member.avatar_url)
        embed.set_footer(text=f'Requested by {ctx.author} at {TimeNahui()}', icon_url=ctx.author.avatar_url)
        embed.add_field(name='Discord Username:',value=f'`{member.display_name}`', inline=False)
        embed.add_field(name='Discord User Discriminator:',value=f'`{member.discriminator}`', inline=False)
        embed.add_field(name='Discord User ID:',value=f'`{member.id}`', inline=False)
        embed.add_field(name='Account Creation Date:',value=f'`{member.created_at.strftime("%A, %B %#d, %Y, %H:%M UTC")}`', inline=False)
        embed.add_field(name=f'Joined {ctx.guild} at:',value=f'`{member.joined_at.strftime("%A, %B %#d, %Y, %H:%M UTC")}`', inline=False)
        #embed.add_field(name=f'Roles ({urb})', value=" ".join([role.mention for role in roles]), inline=False)

        await ctx.send(embed=embed)


@client.command(aliases=["stat", "statistics"])
async def stats(ctx):
    VersionBlyat = platform.python_version()
    DPYBlyat = discord.__version__
    Memberz = MembersBlyat(ctx)
    #Serverz = CServersBlyat()
    embed=discord.Embed(
    title='***KOLBASER***\n*Statistics*',
    colour=0xdd16d9
    )
    embed.add_field(name="Python Version:", value=f"{VersionBlyat}", inline=False)
    embed.add_field(name="Discord.py Version:", value=f"{DPYBlyat}", inline=False)
    embed.add_field(name="Member Count:", value=f"{Memberz}", inline=False)
    #embed.add_field(name="Server Count", value=f"{Serverz}", inline=False)
    await ctx.send(embed=embed)
    return


@client.command(aliases=['trans'])
async def translate(ctx, *, mode):
    embed1 = discord.Embed(
        title='***KOLBASER*** - *Translator*',
        description='Kolbaser is here to assist you in translating whatever the hell you need translated',
        colour=0xdd16d9
    )
    if mode.startswith('langs'):
        codes_lang = '\n'.join(
            [f'{code} -> {country}' for code, country in LANGUAGES.items()])
        await ctx.send(f'```{codes_lang}```')
        return
    elif 'tolang' in mode:
        message, langs = mode.split('tolang')
        src, dest, text = get_translation(message, langs.replace(' ', ''))
        embed1.add_field(name=f'**Translated from `{LANGUAGES[src].title()}` to `{LANGUAGES[dest].title()}`**',
                         value=f'```{text}```',
                         inline=True)
        await ctx.send(embed=embed1)
        return
    else:
        src, dest, text = get_translation(mode)
        embed1.add_field(name=f'*Translated from ```{LANGUAGES[src.lower()].title()}``` to ```{LANGUAGES[dest.lower()].title()}```*',
                         value=f'```{text}```',
                         inline=True)
        await ctx.send(embed=embed1)
        return


@client.command(aliases=["def"])
async def define(ctx, *, term=None):
    if term is None:
        await ctx.send("You need to pass in a word, debil!")
        return
    else:
        x = get_urban(term)
        embed = discord.Embed(
                title='***KOLBASER***\n*Define*',
                description=f'**Word**: `{term}`',
                colour=0xdd16d9
            )
        embed.add_field(name='Defined with URBAN DICTIONARY',
                            value=x, inline=False)
        await ctx.send(embed=embed)
        return

@define.error
async def define_error(ctx, error):
    if isinstance(error, discord.ext.commands.errors.CommandInvokeError):
        await ctx.send("TypeError: object of type 'NoneType' has no len()\nOR\nDefinition of this word is too long, I cannot send it\nOR\nThere are no definitions for the term")
        return


@client.command(pass_context=True)
async def code(ctx, *, cc: str):
    """
    Code Execution only for python code
    :param ctx:
    :param code_to_compile: the code to compile
    :return: embed
    """
    compiler_return = get_code_compiler(cc)
    emojis = ['\U00002705', '\U0000274C']
    if compiler_return.__contains__('error'.title()):
        reaction = emojis[1]
        color = 0xFF0000
    else:
        reaction = emojis[0]
        color = 0xdd16d9
    embed = discord.Embed(title='Compiler Result',description='Compile code within code', colour=color)
    embed.add_field(name='Given Code:', value=cc, inline=True)
    embed.add_field(name='**Result:**',value=f'{compiler_return}', inline=False)

    msg = await ctx.send(embed=embed)
    await msg.add_reaction(reaction)

#math cmds

@client.command(aliases=["temp_info"])
async def temperature_info(ctx):
    embed = discord.Embed(
        title='***Kolbaser***\n*Temperature Converter*',
        description='Convert Temperature!',
        colour=0xdd16d9
    )
    useage = prompt_and_convert()
    embed.add_field(name='Prompt', value=f'`{useage}`', inline=False)
    embed.add_field(name='How to use',value='`k temperature [temperature] [from scale] [to scale]`', inline=False)
    embed.add_field(name='Example', value='`k temperature 1 35 3` where 1 is `Celcius` and 3 is `Kelvin`', inline=False)
    embed.add_field(name='Output', value='`35.0 in Celsius is 308.15 in Kelvin`', inline=False)
    await ctx.send(embed=embed)


@client.command(aliases=["temp"])
async def temperature(ctx, from_scale: int, from_temp: float, to_scale: int):
    ft, fs, result, ts = convert(from_temp, from_scale, to_scale)
    embed = discord.Embed(
        title='***Kolbaser***\n*Temperature Converter*',
        colour=0xdd16d9
    )
    embed.add_field(name=f'{ft} in {fs} is: ',value=f'`{result}`in `{ts}`')
    await ctx.send(embed=embed)


@temperature.error
async def temp_error(ctx, error):
    embed = discord.Embed(
        title='***Kolbaser***\n*Temperature Converter*',
        description='Convert Temperature or vice versa!',
        colour=0xdd16d9
    )
    useage = prompt_and_convert()
    embed.add_field(name='Prompt', value=f'`{useage}`', inline=False)
    embed.add_field(name='How to use',value='`k temperature [number] [temperature] [to number]`', inline=False)
    embed.add_field(name='Example', value='`k temperature 1 35 3` where 1 is `Celcius` and 3 is `Kelvin`', inline=False)
    embed.add_field(name='Output', value='`35.0 in Celsius is 308.15 in Kelvin`', inline=False)
    await ctx.send(embed=embed)


@client.command(aliases=["bac_info"])
async def alcoholcontent_info(ctx):
    embed = discord.Embed(
        title='***Kolbaser***\nBAC Information',
        description='How to use BAC calculator',
        colour=0xdd16d9
    )

    embed.set_thumbnail(url='https://cdn.discordapp.com/avatars/690305523903758612/a79176bd0251eb2df75c143f016238ee.png?size=256')
    embed.set_author(name='KOLBASER', icon_url='https://cdn.discordapp.com/avatars/690305523903758612/6c2dfebc0f2dc4345840909ee5b68338.png?size=1024')

    embed.add_field(name='How to use',value='`k alcoholcontent [gender] [ounces of alcohol] [weight in pounds]`', inline=False)
    embed.add_field(name='Example:',value='`k alcoholcontent male 6 210 `', inline=False)
    await ctx.send(embed=embed)


@client.command(aliases=["bac"])
async def alcoholcontent(ctx, sex: str, d_amount: Union[int, float], w_amount: Union[int, float]):
    await asyncio.sleep(2)
    if sex == 'male':
        mbac = bac_calc1(d_amount, w_amount)
        embed = discord.Embed(
            title='***Kolbaser***\nBlood Alcohol Concentration',
            colour=0xdd16d9
        )
        embed.add_field(name='**FORMULA**', value='(ounces of alcohol) x (men: 3.75, women: 4.7) ÷ (weight in pounds)', inline=False)
        embed.add_field(name='**Blood Alcohol Concentration**', value="{} A male that weighs `{}` pounds and has consumed `{}` ounces of alcohol's BAC is `{}`".format(ctx.author.mention, w_amount, d_amount, mbac), inline=False)
        await ctx.send(embed=embed)

    elif sex == 'female':
        fbac = bac_calc2(d_amount, w_amount)
        embed = discord.Embed(
            title='***Kolbaser***\nBlood Alcohol Concentration',
            colour=0xdd16d9
        )
        embed.add_field(name='**FORMULA**', value='(ounces of alcohol) x (men: 3.75, women: 4.7) ÷ (weight in pounds)', inline=False)
        embed.add_field(name='**Blood Alcohol Concentration**', value="{} A female that weighs `{}` pounds and has consumed `{}` ounces of alcohol's BAC is `{}`".format(ctx.author.mention, w_amount, d_amount, fbac), inline=False)
        await ctx.send(embed=embed)

    else:
        await ctx.send(f'Invalid parameters {ctx.author.mention} debil!')


@client.command(pass_context=True)
async def math(ctx, method: str, n1: float, n2: float):
    methods = {
        'add': lambda n1, n2: '{:.2f}'.format(n1+n2),
        'sub': lambda n1, n2: '{:.2f}'.format(n1-n2),
        'multi': lambda n1, n2: '{:.2f}'.format(n1*n2),
        'div': lambda n1, n2: '{:.3f}'.format(n1/n2),
        'power': lambda n1, n2: n1**n2
    }
    if method.lower() == 'power':
        if len(str(n1)) > 3 or len(str(n2)) > 3:
            await ctx.send("Nah, that number is too big, debil!")
            return
    if method.lower() == 'div' and n2 == 0:
        await ctx.send("You cannot divide by zero, debil!")
        return

    else:

        await ctx.send(f'`{n1}` {method} `{n2}` is {methods[method](n1,n2)}')


@math.error
async def m_error(ctx, error):
    embed = discord.Embed(title='Usage for math command',description='methods = [add,sub,multi,div,power] [number] [number]',colour=0xdd16d9)
    await ctx.send(embed=embed)


@client.command(aliases=["quad"])
async def quadratic(ctx, a: Union[int, float], b: Union[int, float], c: Union[int, float]):
    x = quadratic0(a, b, c)
    sol1 = (-b-cmath.sqrt(x))/(2*a)
    sol2 = (-b+cmath.sqrt(x))/(2*a)
    s1 = '{:.3f}'.format(sol1)
    s2 = '{:.3f}'.format(sol2)
    embed = discord.Embed(
        title="***KOLBASER***\nQuadratic Equations based off of 3 inputs",
        description="Your input:\n ```\nA: {}, B: {}, C: {}```\nPlug it in, cyka!\n```\nd = ({}**2) - (4*{}*{})```".format(a, b, c, b, a, c),
        colour=0xdd16d9
    )
    embed.add_field(name='Solutions', value=f'`{s1}` and `{s2}`', inline=False)
    await ctx.send(embed=embed)


@client.command(aliases=["intcalc", "simpint"])
async def interest(ctx, P: Union[int, float], Rate: Union[int, float], Time: Union[int, float], Com=None):

    if Com == 'normal':
            x = simpint(P, Rate, Time)
            embed = discord.Embed(
                title='***KOLBASER***\nInterest Calculator',
                description='Formula:\n`I = PrT`\n`Interest = {}*{}*{}`'.format(P, Rate, Time),
                colour=0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Interest: `${x}`', inline=False)
            await ctx.send(embed=embed)
            return
    elif Com == 'continuously':
            c = compint(P, Rate, Time)
            embed = discord.Embed(
                title='***KOLBASER***\nCompound Interest Calculator',
                description='Formula:\nA = Pe^(rt)\n`Interest = {}*e**({}*{})`'.format(P, Rate, Time),
                colour=0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` compounded {Com} over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Principal + Interest: `${c}`', inline=False)
            await ctx.send(embed=embed)
            return
    elif Com == 'daily':
            d = compint2(P, Rate, Time)
            embed = discord.Embed(
                title='***KOLBASER***\nDaily Compound Interest Calculator',
                description='Formula:\nA = P(1 + r/n)^(nt)\n`Interest = {}(1 + {}/365)^365`'.format(P, Rate),
                colour=0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` compounded {Com} over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Principal + Interest: `${d}`', inline=False)
            await ctx.send(embed=embed)
            return
    elif Com == 'weekly':
            w = compint3(P, Rate, Time)
            embed = discord.Embed(
                title = '***KOLBASER***\nWeekly Interest Calculator',
                description = 'Formula:\nA = P(1 + r/n)^(nt)\n`Interest = {}(1+{}/52)^52.5`'.format(P, Rate),
                colour = 0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` compounded {Com} over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Principal + Interest: `${w}`', inline=False)
            await ctx.send(embed=embed)
            return
    elif Com == 'monthly':
            m = compint4(P, Rate, Time)
            embed = discord.Embed(
                title = '***KOLBASER***\nMonthly Interest Calculator',
                description = 'Formula:\nA = P(1 + r/n)^(nt)\n`Interest = {}(1+{}/52)^52.5`'.format(P, Rate),
                colour = 0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` compounded {Com} over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Principal + Interest: `${m}`', inline=False)
            await ctx.send(embed=embed)
            return
    elif Com == 'yearly':
            y = compint5(P, Rate, Time)
            embed = discord.Embed(
                title = '***KOLBASER***\nYearly Interest Calculator',
                description = 'Formula:\nA = P(1 + r/n)^(nt)\n`Interest = {}(1+{}/52)^52.5`'.format(P, Rate),
                colour = 0xdd16d9
            )
            embed.add_field(name='Logic:', value=f'`${P}` with an interest rate of `{Rate}` compounded {Com} over a period of `{Time}` years', inline=False)
            embed.add_field(name='Result:', value=f'Principal + Interest: `${y}`', inline=False)
            await ctx.send(embed=embed)
            return


@client.command(aliases=['energy', 'econvert'])
async def energyconvert(ctx, oge: str, ogea: Union[int,float], to_e: str):
    if oge == 'eu' and to_e == 'rf': #eu to rf
        if ogea is not None:
            x1 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour = 0xdd16d9
            )
            embed.add_field(name=f'{ogea} EU converted to {to_e} is:', value=f'{x1} Redstone Flux')
            await ctx.send(embed=embed)
            return

    elif oge == 'eu' and to_e == 'mj': #eu to mj
        if ogea is not None:
            x1 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour = 0xdd16d9
            )
            embed.add_field(name=f'{ogea} EU converted to {to_e} is:', value=f'{x1} Minecraft Joules')
            await ctx.send(embed=embed)
            return

    elif oge == 'mj' and to_e == 'rf': #mj to rf
        if ogea is not None:
            x2 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour=0xdd16d9
            )
            embed.add_field(name=f'{ogea} MJ converted to {to_e} is:', value=f'{x2} Redstone Flux')
            await ctx.send(embed=embed)
            return

    elif oge == 'mj' and to_e == 'eu': #mj to eu
        if ogea is not None:
            x2 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour=0xdd16d9
            )
            embed.add_field(name=f'{ogea} MJ converted to {to_e} is:', value=f'{x2} Energy Units')
            await ctx.send(embed=embed)
            return

    elif oge == 'rf' and to_e == 'mj': #rf to mj
        if ogea is not None:
            x2 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour=0xdd16d9
            )
            embed.add_field(name=f'{ogea} RF converted to {to_e} is:', value=f'{x2} Minecraft Joules')
            await ctx.send(embed=embed)
            return

    elif oge == 'rf' and to_e == 'eu': #rf to eu
        if ogea is not None:
            x2 = converter(oge, ogea, to_e)
            embed = discord.Embed(
                title='***KOLBASER***\n*Minecraft Energy Converter*',
                description='Convert between minecraft energy forms',
                colour=0xdd16d9
            )
            embed.add_field(name=f'{ogea} RF converted to {to_e} is:', value=f'{x2} Energy Units')
            await ctx.send(embed=embed)
            return

    else:
        await ctx.send("Invalid Parameters")


# End of math commands



@client.command()
async def spam(ctx, spam: Union[int,str], num1: int):
    r_words = spam * num1
    if len(r_words) > 2000:
        await ctx.send(f"{ctx.author.mention} debil! I cannot send messages over 2000 characters!")
        return
    else:
        await ctx.send(f'{r_words}')


@spam.error
async def spam_error(ctx,error):
    embed = discord.Embed(title='Usage',description='k [word] [number] -> k something 100',colour=0xdd16d9)
    await ctx.send(embed=embed)


@client.command()
async def kolbasa(ctx):
    kpic = get_kolbasa()
    await asyncio.sleep(.2)
    await ctx.send('***Kolbaser***\nKolbasa')
    await ctx.send(file=discord.File(kpic))
    await asyncio.sleep(1)


@client.command(pass_context=True)
async def meme(ctx):
    meme = get_meme_image()
    MA = MemeAmount()
    await asyncio.sleep(.2)
    await ctx.send(f'***Kolbaser***\n*Meme*\n1 of {MA}')
    MemeKek = await ctx.send(file=discord.File(meme))
    await MemeKek.add_reaction('👍')
    await MemeKek.add_reaction('👎')


@client.command(pass_context=True)
async def flbp(ctx):
    flbp = get_boob_image()
    #flbps = FLBPAmount()
    await asyncio.sleep(.2)
    await ctx.send(f'***Kolbaser***\nFLBP')
    await ctx.send(file=discord.File(flbp))
    await asyncio.sleep(1)


@client.command(pass_context=True)
async def vid(ctx):
    fv = get_video()
    VA = VidAmount()
    await asyncio.sleep(.2)
    await ctx.send(f'***Kolbaser***\nVideo\n1 of {VA}')
    VideoKek = await ctx.send(file=discord.File(fv))
    await VideoKek.add_reaction('😆')
    await VideoKek.add_reaction('😐')
    await VideoKek.add_reaction('😱')


@client.command(pass_context=True)
async def roulette(ctx):
    await ctx.send(f'You have started a game of Russian Roulette! {ctx.author.mention}\nType `k spin` to play!')
    @client.command(pass_context=True)
    async def spin(ctx):
        await ctx.send(f'Spinning...')
        await asyncio.sleep(3.2)
        await ctx.channel.purge(limit=1)
        await ctx.send('Ready when you are cyka\nType `k fire`')
        @client.command(pass_context=True)
        async def fire(ctx):
            responses = ["Safe!",
                         "Close one",
                         "You are a very lucky guy",
                         "Miss!",
                         "**BANG!**",
                         "Oof, you just took a bullet to the head",
                         "You got scared and moved, so the bullet only grazed you",
                         "**BOOM** HEADSHOT!",
                         "Aw shit, you got your blood all over my new shoes!",
                         "R.I.P",
                         "Lucky you, the gun jammed",
                         "I will send flowers and my sincerest condolences to your family",
                         "You lucky fuck",
                         "The bullet went straight through your temple, killing you instantly"]

            await asyncio.sleep(1.5)
            await ctx.channel.send(f'{random.choice(responses)} {ctx.author.mention}\nType `k spin` to play again!')


@client.command(aliases=['8ball'])
async def ask(ctx, *, question):
    responses = ["It is certain",
                 "Yeah pretty much",
                 "Damn straight",
                 "Duh, blyat",
                 "You can count on it!",
                 "blin, i dont give a fuck",
                 "What was the question again?",
                 "Nah",
                 "Double Nah",
                 "I like bread",
                 "Idi nahui",
                 "Opa blja!",
                 "Yes",
                 "Blyat cyka urod",
                 "Blyat how the fuck should i know?",
                 "ERROR, 8BALL IS TEMPORAILY OUT-OF-ORDER, TRY AGAIN LATER",
                 "Da, pidoras",
                 "Better not tell you now, cyka blyat",
                 "Does a bear shit in the woods?",
                 "Invalid parameters",
                 "Why are you asking me this",
                 "I think not",
                 "Depends...",
                 "нет",
                 "Too drunk too answer your question, ask again NEVER",
                 "Why are you asking me, blin?"]
    await ctx.send(f'Question: {question}\nAnswer: {random.choice(responses)}')


@client.command(aliases=['dox'])
async def hack(ctx, member: discord.Member = None):
    fake_passwords = ["`quertyfuck`", "`i-like-men69`",
                      "`i_haz_small_dick111`", "`asdfghjkl69`", "`xxx6969420xXx`"]
    fake_emails = ["`xxxgay4idriselbaxxx@hotmail.com`", "`xxx_i_like_boyz@gmail.com`",
                   "`xGay4Trump@aol.com`", "`hf6969@gmail.com`", "`x6hawtlesboactionx@hotmail.com`"]
    fake_device_names = ["`Gayfucker`", "`bond007`",
                         "`fortnite4life`", "`dicks`", "`Idris_Elba`", "`White4Lyfe`"]
    fake_ssns = ["`666-420-6969`", "`123-419-8886`",
                 "`069-690-8008`", "`123-456-6969`", "`420-420-1111`"]
    fake_device_passwords = ["`123456789`", "`Monk3yFuck3r`",
                             "`penisface`", "`i_like_2_suck_dix`", "`Gay4IdrisElba`"]
    fake_ips = ["`173.81.201.100`", "`184.250.125.80`", "`190.187.192.102`", "`144.94.178.85`",
                "`169.28.29.420`", "`30.130.17.31`", "`170.236.177.192`", "`110.225.211.247`", "`189.196.208.11`"]
    fake_addresses = ["`123 Happy Street`", "`239 Walt Whitman St.Fuckface, NJ 07006`",
                      "`862 Wayne St.Land O Niggaz, FL 34639`", "`9631 NW. Penis licker Road, MD 21221`"]
    fake_names = ["`A.S. Muncher`", "`Amanda D. P. Throat`", "`Ben O. Verbich`", "`Anita Hanjaab`", "`Buster Himen`",
                  "`Dick Pound`", "`Dixon B. Tweenerlegs`", "`Harry Johnson`", "`Ben Dover`", "`Miley Cyrus`", "`Hernie Clanders`"]
    if not member:
        await ctx.send("You must specifiy who you want to hack, randy")
        return
    elif member.id == HF:
        await ctx.send("Cannot hack the Overlord, stupid debil!")
        return
    elif member.id == Kolbaser:
        await ctx.send("No")
        return
    else:
        await ctx.send(f'Hacking {member.mention}')
        await asyncio.sleep(1.5)
        await ctx.send(f'Getting discord email account name....')

        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Discord email account name: {random.choice(fake_emails)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting discord email account password....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Discord email account password: {random.choice(fake_passwords)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting discord user device name....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Discord user device name: {random.choice(fake_device_names)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting user device password....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Discord user device password: {random.choice(fake_device_passwords)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting user social security number....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Social security number: {random.choice(fake_ssns)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting user IP address....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'IP Address: {random.choice(fake_ips)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting user address....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Address: {random.choice(fake_addresses)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Getting name....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Name: {random.choice(fake_names)}')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Registering user as a sex offender....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Successfully registered user as a sex offender')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Adding user to No-Fly list')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.channel.send(f'Successfully added user to No-Fly list')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Leaking data....')
        await asyncio.sleep(2.5)
        await ctx.channel.purge(limit=1)
        await ctx.send(f'Leaked data to FBI, CIA, DEA, ATF, NSA, KKK, KGB...')
        await asyncio.sleep(2)
        await ctx.channel.purge(limit=1)

        await ctx.send(f'Hack complete!')
        return


@client.command(pass_context=True)
async def kill(ctx, member: discord.Member = None):
    if not member:
        await ctx.send(UserDebil)
        return
    else:
        if member.id == HF:
            await ctx.send("Cannot kill my Lord and Savior")
        elif member.id == Kolbaser:
            await ctx.send("No")

        else:
            await ctx.send(f"*{member} has been killed*\n||jk lol||")


@client.command(aliases=["avatar"])
async def av(ctx, member: discord.Member = None):
    if not member:
        await ctx.send(UserDebil)
        return
    else:
        show_avatar = discord.Embed(

            color=0xdd16d9
        )
        show_avatar.set_image(url='{}'.format(member.avatar_url))
        await ctx.send(embed=show_avatar)
        return


@client.command()
async def say(ctx, *, message=None):
    if not message:
        await ctx.send(oDebil)
        return
    rolegg = discord.utils.get(ctx.guild.roles, name="saltwater nigger")
    if rolegg in ctx.author.roles:
        await ctx.send("personalizinq is gay")
        return
    else:
        try:
            await ctx.channel.purge(limit=1)
        except Exception:
            pass

        embed = discord.Embed(
            description=message,
            colour=0xdd16d9

            )
        await ctx.channel.send(embed=embed)
        return


@client.command(pass_context=True)
async def ping(ctx):
    await ctx.send(f'Dont ping me, blyat!\n\n{round(client.latency * 1000)} Milliseconds')


@client.command()
async def count(ctx, *args):
    await ctx.send('{} arguments: {}'.format(len(args), ', '.join(args)))


@client.command()
async def servers(ctx):
    def CServersBlyat():
         D1 = len(client.guilds)
         return D1
    D2 = CServersBlyat()
    await ctx.send(f'Kolbaser is in {D2} servers')


@client.command(aliases=["c_users"])
async def users(ctx):
    CU = sum([len(guild.members) for guild in client.guilds])
    await ctx.send(CU)


@client.command()
async def suggest(ctx, *, msg: str = None):
    if not msg:
        await ctx.send(oBlyat)
    else:
        server = client.get_guild(681262466650734596)
        channel = client.get_channel(699655178818814013)
        embed = discord.Embed(
            title=f'{msg}',
            description=f'Sent by: **{ctx.author}**\nAuthor ID:`{ctx.author.id}`\nServer: `{ctx.guild}`\nServer ID: `{ctx.guild.id}`',
            colour=0xdd16d9
        )
        await channel.send(embed=embed)
        await ctx.send("Submitted")


@client.command()
async def membercount(ctx):
    bots = len([bot for bot in ctx.guild.members if bot.bot])
    members = len([member for member in ctx.guild.members if not member.bot])
    await ctx.send(f'There are {bots} bots and {members} members in {ctx.guild}\nTotal members: {bots + members}')


@client.command()
async def joke(ctx):
    key, value = get_joke()

    embed = discord.Embed(
        title='***Kolbasers Joke Hub***',
        description='Хороший день для тебя!\nGood day to you!',
        colour=0xdd16d9)
    await asyncio.sleep(2)
    embed.add_field(name='***Kolbaser*** *Joke*',
                    value='Here, have a joke and иди на хуй', inline=False)
    embed.add_field(name=f'{key}', value=f'||{value}||')

    await ctx.send(embed=embed)


@client.command()
async def ch(ctx):
    role = discord.utils.get(ctx.guild.roles, name='sdasdas')
    print(role)
    channel = discord.utils.get(ctx.guild.text_channels, name='gulag_prison', colour=0xdd16d9)
    print(channel)

client.remove_command('help')
###############################################################################
key='NjkwMzA1NTIzOTAzNzU4NjEy.XnPfHg.rH56qgfjMAyeUj1-Ij1zqcGll20'
client.run(key)