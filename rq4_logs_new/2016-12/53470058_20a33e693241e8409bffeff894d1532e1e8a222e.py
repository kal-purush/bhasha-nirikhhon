import discord
from discord.ext import commands as cmds


class Example518:
    def __init__(self, bot):
        self.bot = bot
        self.conf = bot.get_conf(self.__class__.__name__, 23894723987423987)

        self.conf.registerGlobal("is_ready", False)
        self.conf.registerServer("is_server_enabled", False)
        self.conf.registerChannel("is_channel_enabled", False)
        self.conf.registerRole("is_role_enabled", False)
        self.conf.registerMember("is_member_enabled", False)
        self.conf.registerUser("is_user_enabled", False)

    @cmds.command()
    async def botready(self):
        txt = "is" if self.conf.is_ready else "is not"
        await self.bot.say("Bot {} ready.".format(txt))

    @cmds.command(pass_context=True)
    async def serverenablecheck(self, ctx, set: bool=None):
        if set is not None:
            self.conf.server(ctx.message.server).set("is_server_enabled", set)
        txt = "is" if self.conf.server(ctx.message.server).is_server_enabled \
            else "is not"
        await self.bot.say("Bot {} server enabled.".format(txt))

    @cmds.command(pass_context=True)
    async def channelenablecheck(self, ctx, set: bool=None):
        if set is not None:
            self.conf.channel(ctx.message.channel).set(
                "is_channel_enabled", set)
        txt = "is" if self.conf.channel(
            ctx.message.channel).is_channel_enabled \
            else "is not"
        await self.bot.say("Bot {} channel enabled.".format(txt))

    @cmds.command(pass_context=True)
    async def roleenablecheck(self, ctx, role: discord.Role, set: bool=None):
        if set is not None:
            self.conf.role(role).set(
                "is_role_enabled", set)
        txt = "is" if self.conf.role(
            role).is_role_enabled \
            else "is not"
        await self.bot.say("Bot {} role enabled.".format(txt))