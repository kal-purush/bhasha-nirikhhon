import keyboard as kb
import time
import discord
import asyncio


class SelfBotStatus:
    def __init__(self, bot):
        self.bot = bot

        self.is_online = True

        self._last_time = 0

        kb.hook(self.on_kb_press)

        self.status_task = None

    def __unload(self):
        try:
            self.status_task.cancel()
        except AttributeError:
            pass

    def on_kb_press(self, name, scan_code, time):
        diff = time - self._last_time
        if self.is_online is True and diff > 30:
            self.status_task.cancel()
            self.status_task = self.bot.loop.create_task(
                self._set_idle(time + 300))
        elif self.is_online is False:
            self.status_task = self.bot.loop.create_task(
                self._set_online())
        self._last_time = time

    def _get_game(self):
        try:
            me = self.bot.servers[0].me
            game = me.game
        except IndexError:
            game = None

        return game

    async def _set_idle(self, ttl):
        while time.time() < ttl:
            asyncio.sleep(1)
        game = self._get_game()
        await self.bot.change_presence(game=game, status=discord.Status.idle,
                                       afk=True)

    async def _set_online(self, ttl=0):
        while time.time() < ttl:
            asyncio.sleep(1)
        game = self._get_game()
        await self.bot.change_presence(game=game, status=discord.Status.online,
                                       afk=False)


def setup(bot):
    bot.add_cog(SelfBotStatus(bot))