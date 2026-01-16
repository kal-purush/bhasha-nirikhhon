#!/home/deploy/bots/pvs-bot/discord/bin/python
import discord
import asyncio
import requests
import datetime
import urllib.parse
import urllib.request
import id

client = discord.Client()
# TODO: TESTING
# TODO: !timeout

# Rank Getter for verify()
URL = {
    'base': 'https://{proxy}.api.pvp.net/api/lol/{region}/{url}',
    'summoner_by_name': 'v{version}/summoner/by-name/{names}',
    'get_league': 'v{version}/league/by-summoner/{id}/entry',
    'get_runes': 'v{version}/summoner/{id}/runes'
}

API_VERSIONS = {
    'summoner_by_name': '1.4',
    'get_league': '2.5',
    'get_runes': '1.4'
}
RL = {
    'base': 'https://{proxy}.api.pvp.net/api/lol/{region}/{url}',
    'summoner_by_name': 'v{version}/summoner/by-name/{names}',
    'get_league': 'v{version}/league/by-summoner/{id}/entry',
    'get_runes': 'v{version}/summoner/{id}/runes'
}

API_VERSIONS = {
    'summoner_by_name': '1.4',
    'get_league': '2.5',
    'get_runes': '1.4'
}


class RankGetter(object):
    def __init__(self, api_key):
        self.api_key = api_key

    def _request(self, api_url, region, params={}):
        args = {'api_key': self.api_key}
        for key, value in params.items():
            if key not in args:
                args[key] = value
        destination = URL['base'].format(
            proxy=region,
            region=region,
            url=api_url
        )
        response = requests.get(
            destination,
            params=args
        )
        return response.json()

    # Based on Summoner name, get their info
    def _get_summoner_by_name(self, name, region):
        api_url = URL['summoner_by_name'].format(
            version=API_VERSIONS['summoner_by_name'],
            names=name)
        return self._request(api_url, region)

    # Based on Summoner name, get ID
    def _get_summoner_id(self, name, region):
        self.name = name.replace(" ", "").lower()
        r = self._get_summoner_by_name(self.name, region)[self.name]
        return r['id']

    # Based on Summoner ID, get their rank
    def _get_rank(self, id, region):
        try:
            api_url = URL['get_league'].format(
                version=API_VERSIONS['get_league'], id=id)
            league = self._request(api_url, region.rstrip())[str(id)][0]
            tier = league['tier']
            division = league['entries'][0]['division']
            return tier.title()  # + " " + division
        except:
            return "Unranked"

    # Get information based on rune page
    def _get_runes(self, id, region):
        api_url = URL['get_runes'].format(
            version=API_VERSIONS['get_runes'],
            id=id)
        return self._request(api_url, region)

    # Get first rune page name
    def _get_rune_name(self, id, region):
        runes = self._get_runes(id, region)
        return runes[str(id)]['pages'][0]['name']

    # Returns account info that was saved (author'sName,summoner'sName,region)
    def _get_linked_account(self, name, author, region):
        id = self._get_summoner_id(name, region)
        accountLink = [[i for i in line.split(',')] for line in
                       open('linkedAccounts.txt', 'r')]
        for x in accountLink:
            if x[1] == str(id):
                if x[0].lower() == author.lower():
                    if x[2].rstrip() == region.lower():
                        return x

    def verify_rank(self, name, region):
        id = self._get_summoner_id(name, region)
        if self._get_rune_name(id, region) == "summonersplaza":
            return self._get_rank(id, region)
        else:
            return "Error"


rg = RankGetter('070e0d5e-c950-47f5-8a6c-fb3a5861f70c')
ranks = ["Bronze", "Silver", "Gold", "Platinum", "Diamond", "Masters",
         "Challenger"]


@asyncio.coroutine
def verify(message):  # check elo and assign role
    try:
        author = message.author
        content = message.content[8:].split(',')
        rank = rg.verify_rank(content[0], content[1].lower().strip(" "))
        rank2 = discord.utils.get(message.server.roles, name=rank)
        region = discord.utils.get(message.server.roles,
                                   name=content[1].upper().strip(" "))
        roles = author.roles
        # if rank == "Masters" or rank == "Challenger" or rank == "Diamond":
        #    rank2 = discord.utils.get(message.server.roles, name='Diamond +')
        if rank == "Error":
            yield from client.send_message(message.channel, "To verify rank, "
                                      "please set the name of your first "
                                      "rune page to 'summonersplaza'.")
        elif rank == "Unranked":
            yield from client.send_message(message.channel,
                                           "You are not ranked in "
                                           "dynamic queue.")
        else:
            roleList = [discord.utils.get(message.server.roles,
                        name='Verified'), rank2, region]
            for r in message.author.roles:
                if r.name not in ranks:
                    roleList.append(discord.utils.get(message.server.roles,
                                                      name=r.name))
            yield from client.replace_roles(author, *roleList)
            yield from client.send_message(message.channel,
                                           "You have been added to {}".format(
                                               rank2))
    except:
        yield from client.send_message(message.channel, "Invalid format."
                                       "Correct syntax is `!verify "
                                       "summoner,region`")


# lists of roles to check against
assignable_roles = ['NA', 'EUW', 'EUNE', 'OCE', 'BR', 'LAN', 'LAS', 'CHINA',
                    'KR', 'Turkey', 'GARENA', 'Top', 'Mid', 'Jungle', 'ADC',
                    'Support', 'Bonze', 'Silver', 'Gold', 'Platinum',
                    'Diamond +', 'Coach', 'NLFG']
privileged_roles = ['admin', 'Moderator']
rank_roles = ['Bonze', 'Silver', 'Gold', 'Platinum', 'Diamond +']


# function for generic role self-add
async def role_add(message):
    author = message.author
    user_input = message.content[2:]
    role = discord.utils.get(message.server.roles, name=user_input)
    roleLow = discord.utils.get(message.server.roles, name=user_input.lower())
    roleUpp = discord.utils.get(message.server.roles, name=user_input.upper())
    roleTit = discord.utils.get(message.server.roles, name=user_input.title())
    verified_role = discord.utils.get(message.server.roles, name='Verified')

    if str(role) != "None" and str(role) in assignable_roles:
        await client.add_roles(author, role)
        await client.send_message(message.channel, "You have been added to {}"
                                  .format(role))
        if str(role) in rank_roles:
                await client.remove_roles(author, verified_role)
    elif str(roleLow) != "None" and str(roleLow) in assignable_roles:
        await client.add_roles(author, roleLow)
        await client.send_message(message.channel, "You have been added to {}"
                                  .format(roleLow))
        if str(roleLow) in rank_roles:
                await client.remove_roles(author, verified_role)
    elif str(roleUpp) != "None" and str(roleUpp) in assignable_roles:
        await client.add_roles(author, roleUpp)
        await client.send_message(message.channel, "You have been added to {}"
                                  .format(roleUpp))
        if str(roleUpp) in rank_roles:
                await client.remove_roles(author, verified_role)
    elif str(roleTit) != "None" and str(roleTit) in assignable_roles:
        await client.add_roles(author, roleTit)
        await client.send_message(message.channel, "You have been added to {}"
                                  .format(roleTit))
        if str(roleTit) in rank_roles:
                await client.remove_roles(author, verified_role)
    else:
        await client.send_message(message.channel,
                                  "Please enter a valid role.")


# function for generic role self-removal
async def role_strip(message):
    author = message.author
    user_input = message.content[2:]
    role = discord.utils.get(message.server.roles, name=user_input)
    roleLow = discord.utils.get(message.server.roles, name=user_input.lower())
    roleUpp = discord.utils.get(message.server.roles, name=user_input.upper())
    roleTit = discord.utils.get(message.server.roles, name=user_input.title())

    if str(role) != "None":
        await client.remove_roles(author, role)
        await client.send_message(message.channel, "You have been removed from"
                                  " {}".format(role))
    elif str(roleLow) != "None":
        await client.remove_roles(author, roleLow)
        await client.send_message(message.channel, "You have been removed from"
                                  " {}".format(roleLow))
    elif str(roleUpp) != "None":
        await client.remove_roles(author, roleUpp)
        await client.send_message(message.channel, "You have been removed from"
                                  " {}".format(roleUpp))
    elif str(roleTit) != "None":
        await client.remove_roles(author, roleTit)
        await client.send_message(message.channel, "You have been removed from"
                                  " {}".format(roleTit))
    else:
        await client.send_message(message.channel,
                                  "Please enter a valid role.")


# function to format and filter log in #chatlog and return a string
async def savelogs(message):
    logs = []
    command, channel, numberString = message.content.split(' ')
    chatlog = discord.utils.get(message.server.channels, name='chatlog')
    s = ""
    channel = channel.lower()
    try:
        number = int(numberString)
        async for log in client.logs_from(chatlog, limit=number):
            if ("**" + channel + "**") in log.content:
                logs.append(log)
        for l in reversed(logs):
            s += l.content + "\n"
        return channel, s
    except:
        return channel, s


# function to format and filter log in #chatlog and return a list
async def savelogs2(message):
    logs = []
    command, channel, numberString = message.content.split(' ')
    chatlog = discord.utils.get(message.server.channels, name='chatlog')
    s = ""
    channel = channel.lower()
    try:
        number = int(numberString)
        async for log in client.logs_from(chatlog, limit=number):
            if ("**" + channel + "**") in log.content:
                logs.append(log)
        return channel, reversed(logs)
    except:
        return channel, logs


# function to upload output from savelogs(2)() to pastebin
async def pastbin(title, content):
    pastebin_vars = dict(
        api_option='paste',
        api_dev_key='2e15e96203dacd86c46417862c41f10f',
        api_paste_name=title,
        api_paste_code=content
    )
    return urllib.request.urlopen('http://pastebin.com/api/api_post.php',
                                  urllib.parse.urlencode(pastebin_vars).encode(
                                      'utf-8')).read()
"""# kick command
async def kick_user(message):
    content = message.content[5:].strip()
    target = discord.utils.get(message.server.members, display_name=content)
    if target is None:
        target = discord.utils.get(message.server.members, name=content)
    await client.start_private_message(target)
    await client.send_message(target, 'You got kicked from PvS by {}'
                              .format(message.author.display_name))
    await client.kick(target)"""


@client.event
async def on_member_join(member):
    chatlog = discord.utils.get(member.server.channels, name='chatlog')
    await client.send_message(chatlog, "{} UTC: `JOINED` {}".format(timestamp,
                                                                    member))


@client.event
async def on_member_remove(member):
    chatlog = discord.utils.get(member.server.channels, name='chatlog')
    await client.send_message(chatlog, "{} UTC: `LEFT` {}".format(timestamp,
                                                                  member))


@client.event
@asyncio.coroutine
def on_member_update(before, after):
    chatlog = discord.utils.get(before.server.channels, name='chatlog')
    name_before = before.display_name
    name_after = after.display_name
    if name_after is not None and name_before is not None and \
            name_before != name_after:
        yield from client.send_message(chatlog, "{} UTC: `NICKNAME CHANGED` "
                                  "({}) from {} to {}".format(timestamp,
                                                             before,
                                                             name_before,
                                                             name_after))


@client.event
@asyncio.coroutine
def on_message_delete(message):
    chatlog = discord.utils.get(message.server.channels, name='chatlog')
    timestamp = message.timestamp.strftime('%b %d: %H:%M')
    if str(message.channel) != 'chatlog':
        yield from client.send_message(chatlog, "{} UTC: `DELETED` **{}:** {}:"
                                       " {}".format(timestamp, message.channel,
                                          message.author,
                                          message.content.replace('@', '@ ')))


@client.event
@asyncio.coroutine
def on_message_edit(before, after):
    chatlog = discord.utils.get(before.server.channels, name='chatlog')
    timestamp = before.timestamp.strftime('%b %d: %H:%M')
    channel = before.channel
    author = before.author
    content_before = before.content.replace('@', '@ ')
    content_after = after.content.replace('@', '@ ')
    if str(channel) != 'chatlog':
        yield from client.send_message(chatlog, "{} UTC: `EDITED`\n\t `BEFORE`"
                                  "**{}:** {}: {}\n\t`AFTER` **{}:** {}: {}"
                                  .format(timestamp, channel, author,
                                          content_before, channel, author,
                                          content_after))


@client.event
@asyncio.coroutine
def on_message(message):
    chatlog = discord.utils.get(message.server.channels, name='chatlog')
    timestamp = message.timestamp.strftime('%b %d: %H:%M')
    content = message.content
    channel = message.channel
    roleAssignmentChannel = discord.utils.get(message.server.channels,
                                              name='role-assignment')
    if str(channel) != 'chatlog':   # bot reposts everything not in chatlog
        yield from client.send_message(chatlog, "{} UTC: `SENT` **{}:** {}: {}"
                                    .format(timestamp, channel, message.author,
                                            content.replace('@', '@ ')))
    if content.startswith('+!'):
        if channel == roleAssignmentChannel:
            if content[2:].lower() == 'coach':
                for r in message.author.roles:
                    if r.name == 'Diamond +' or r.name == 'Platinum':
                        high_elo = True
                if high_elo:
                    for r in message.author.roles:
                        if r.name == 'Verified':
                            unverified = False
                        else:
                            unverified = True
                    if unverified:
                        yield from client.send_message(channel,
                                            "Please verify your rank first.")
                    else:
                        yield from role_add(message)
                else:
                        yield from client.send_message(channel,
                            "You don't meet our elo requirement to self "
                            "assign the coach role. You need to be at least "
                            "Platinum.")
            else:
                yield from role_add(message)

    elif content.startswith('-!'):
        if channel == roleAssignmentChannel:
            yield from role_strip(message)

    elif content.startswith('??help') and channel == roleAssignmentChannel:
        yield from client.send_message(channel,
                                  "You can add yourself to roles by "
                                  "typing `+!ROLE` and remove yourself with "
                                  "``-!ROLE`. See `??roles` for a list of "
                                  "assignable roles. You can also verify your"
                                  " league by using `!verify summonername,"
                                  "region`. Use `??verify` to learn more.")
    elif content.startswith('??verify') and channel == roleAssignmentChannel:
        yield from client.send_message(channel,
                                  "To verify your account follow these"
                                  "steps:\n1.: Rename your first rune page to"
                                  "'summonersplaza'\n2.: Type `!verify "
                                  "summonername,region`. Verification is not "
                                  "available for all regions.")

    elif content.startswith('??roles') and channel == roleAssignmentChannel:
            yield from client.send_message(channel,
                                      "Roles you can use me for: "
                                      "`{}`, `{}`, `{}`, `{}`, `{}`, `{}`,"
                                      " `{}`, `{}`, `{}`, `{}`, `{}`, `{}`,"
                                      " `{}`, `{}`, `{}`, `{}`, `{}`, `{}`,"
                                      " `{}`, `{}`, `{}`, `{}`, `{}`"
                                      .format(*assignable_roles))
    elif content.lower().startswith('!verify'):
        yield from verify(message)

    elif content.startswith('!savelogs'):
        for r in message.author.roles:
            if r.name in privileged_roles:  # only works for whitelisted roles
                channel, logs = yield from savelogs(message)
                if logs == '':
                    yield from client.send_message(message.channel, "An error "
                                              "occured. Please make sure you "
                                              "are using the correct syntax"
                                              "`!savelogs channel number`")
                else:
                    title = "Chatlog for {}".format(channel)
                    paste_link = yield from pastebin(title, logs)
                    paste_link.decode('utf-8')
                    yield from client.send_message(message.channel,
                                                   "Here is the chatlog: {}"
                                                   .format(paste_link))
            else:
                yield from client.send_message(message.channel,
                                               "You don't have permission to "
                                               "request chatlogs.")

    """
    elif content.startswith('!kick'):
        async for r in message.author.roles:
            if r.name in privileged_roles:
                await kick_user(message)
            else:
                await client.send_message(message.channel, "You lack "
                                          "permission to kick users. This "
                                          "incident will be reported.")
                admin_channel = discord.utils.get(message.server.channels,
                                                  name='admin')
                await client.send_message(admin_channel, "{} tried to use "
                "`!kick` in {}".format(message.author,message.channel))"""


client.run(id.token1())