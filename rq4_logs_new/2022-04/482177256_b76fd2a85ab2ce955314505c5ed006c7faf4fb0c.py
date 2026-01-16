from typing import TYPE_CHECKING

from discord.ext import commands

if TYPE_CHECKING:
    from peterbot import PeterBot


class OnHandling(commands.Cog):
    def __init__(self, bot: "PeterBot"):
        self.bot = bot


async def setup(bot: PeterBot) -> None:
    await bot.add_cog(OnHandling(bot))