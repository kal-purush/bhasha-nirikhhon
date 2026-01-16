import logging
from functools import wraps

logger = logging.getLogger(__name__)


def is_team_allowed(func):
    @wraps(func)
    async def wrapper(self, ctx, *args, **kwargs):
        for role in ctx.author.roles:
            if any(role.name == team["discord"] for _, team in self.bot.config["teams_to_skip"].items()):
                await ctx.send(f"{ctx.author} your team is not allowed to run this command")
                logger.info(f"{ctx.author} your team is not allowed to run this command")
                return None
        return await func(self, ctx, *args, **kwargs)

    return wrapper