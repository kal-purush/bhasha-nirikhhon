import datetime

import discord
from discord.ext import commands, tasks

import helper
import settings
from models.account import Account
from models.assets import Assets

logger = settings.logging.getLogger('bot')


async def setup(bot):
    await bot.add_cog(Actions(bot))


class Actions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.jobs = {
            "Unemployed": {"salary": 0, "requirements": {"balance": 0, "xp": 0}},
            "Dishwasher": {"salary": 725, "requirements": {"balance": 0, "xp": 0}},
            "Burger Flipper": {"salary": 1350, "requirements": {"balance": 75000, "xp": 2500}},
            "Grill Master": {"salary": 1675, "requirements": {"balance": 500000, "xp": 15000}},
            "Shift Lead": {"salary": 2000, "requirements": {"balance": 2500000, "xp": 50000}}
        }
        self.check_action_times.start()

    ## COMMANDS
    @commands.command()
    async def cancel(self, ctx):
        user_id = ctx.author.id
        user = await self.bot.fetch_user(user_id)
        account = await Account.fetch(user_id)

        if not account.action_start:
            await ctx.send("You weren't doing anything to begin with!")
            return

        elapsed_time = (datetime.datetime.utcnow() - account.action_start)
        elapsed_seconds = elapsed_time / datetime.timedelta(seconds=1)

        if elapsed_seconds < 10:
            await ctx.send(f"Please wait at least 10 seconds before cancelling action ({elapsed_seconds:.2}s)")
            return

        if not await helper.confirmation_request(ctx, text=f"Cancel action '{account.action_type}'?", timeout=20,
                                                 user=user):
            return

        await self.pop_action_time(account, elapsed_time)

    @commands.group(invoke_without_command=True, aliases=['w'], help="Usage: !work [Shift Duration (hours)]")
    async def work(self, ctx, num_hours: int = 0):
        user_id = ctx.author.id
        account = await Account.fetch(user_id)

        if account.job_title == "Unemployed":
            await ctx.send("You can't work until you are hired!\n*(Hint: Try !promotion)*")
            return

        if account.action_start is not None:
            await self.send_action_status(ctx, account)
            return

        if num_hours < 1:
            await ctx.send("You have to work at least 1 hour to get paid!")
            return

        if num_hours > 24:
            await ctx.send("You cannot work more than 24 hours in a single shift!")
            return

        await Account.update_acct(account=account, action_start=datetime.datetime.utcnow(), action_length=num_hours,
                                  action_type="work")

        await ctx.send("You started working!")

    @commands.command(aliases=['m'], help="Usage: !mine [Action Duration (hours)]")
    async def mine(self, ctx, num_hours: int = 0):
        user_id = ctx.author.id
        account = await Account.fetch(user_id)

        if account.action_start is not None:
            await self.send_action_status(ctx, account)
            return

        if num_hours < 1:
            await ctx.send("You aughta mine fer at least an hour if yer expectin' any gold or such!")
            return

        if num_hours > 24:
            await ctx.send("Ev'n da finest prospectors inda West can't mine fer longer than uhday!")
            return

        await Account.update_acct(account=account, action_start=datetime.datetime.utcnow(), action_length=num_hours,
                                  action_type="mine")

        await ctx.send("You started mining!")

    ## HELPER METHODS
    async def pop_work_time(self, account, elapsed_time):
        user = await self.bot.fetch_user(account.user_id)
        user_assets = await Assets.fetch(account.user_id)

        elapsed_hours = elapsed_time / datetime.timedelta(hours=1)

        money_earned = elapsed_hours * self.jobs[account.job_title]["salary"]
        xp_earned = int(elapsed_hours * 30)

        await Assets.update_assets(user=user_assets, cash_delta=money_earned)
        await Account.update_acct(account=account, main_xp_delta=xp_earned)

        embed = discord.Embed(
            colour=discord.Colour.from_rgb(77, 199, 222),  # Space Blue
            title=f"Shift for {datetime.datetime.now():%m-%d-%Y}"
        )

        embed.add_field(name="You Earned:", value=f"{Assets.format('cash', money_earned)}\n{xp_earned} xp")
        await user.send(embed=embed)

    async def pop_mine_time(self, account, elapsed_time):
        user = await self.bot.fetch_user(account.user_id)
        user_assets = await Assets.fetch(account.user_id)

        elapsed_hours = elapsed_time / datetime.timedelta(hours=1)

        gold_rate = 0.125
        gold_earned = elapsed_hours * gold_rate
        xp_earned = int(elapsed_hours * 21)

        await Assets.update_assets(user=user_assets, gold_delta=gold_earned)
        await Account.update_acct(account=account, main_xp_delta=xp_earned)

        embed = discord.Embed(
            colour=discord.Colour.from_rgb(153, 101, 21),  # Golden Brown
            title=f"Dig of {datetime.datetime.now():%m-%d-%Y}"
        )

        embed.add_field(name="You Earned:", value=f"{Assets.format('gold', gold_earned)} gold\n{xp_earned} XP")
        await user.send(embed=embed)

    async def pop_action_time(self, account, elapsed_time):
        action_type = account.action_type
        if action_type == "work":
            await self.pop_work_time(account, elapsed_time)
        elif action_type == "mine":
            await self.pop_mine_time(account, elapsed_time)
        await Account.update_acct(account=account, action_start="NULL", action_length="NULL", action_type="NULL")

    async def send_action_status(self, ctx, account):
        elapsed_time = (datetime.datetime.utcnow() - account.action_start)
        elapsed_hours = elapsed_time / datetime.timedelta(hours=1)
        percent_left = (elapsed_hours / account.action_length)

        if percent_left > 1:
            await self.pop_action_time(account, elapsed_time)
            return

        blank_bar = "# [----------------]"
        progress_bar = blank_bar.replace("-", "#", int(blank_bar.count("-") * percent_left))
        elapsed_seconds = elapsed_time / datetime.timedelta(seconds=1)
        et, eu = await self.convert_time(elapsed_seconds, "seconds")

        embed = discord.Embed(
            title=f"{account.action_type.title()} {(percent_left * 100):.2f}% complete",
            colour=discord.Colour.blue(),
            description=progress_bar
        )
        embed.set_footer(text=f"{et:.2f} {eu} out of {account.action_length} hr(s)")
        await ctx.send(embed=embed)

    @staticmethod
    async def convert_time(time, units="seconds"):
        for i in range(2):
            if time >= 60:
                time /= 60
                units = "minutes" if units == "seconds" else "hours"
        return time, units

    ## TASKS
    @tasks.loop(seconds=300)  # Check every 5 minutes
    async def check_action_times(self):
        current_time = datetime.datetime.utcnow()
        for account in Account.select().where(Account.action_start.is_null(False)):
            elapsed_time = (current_time - account.action_start)
            elapsed_hours = elapsed_time / datetime.timedelta(hours=1)
            logger.debug(f"{account.user_id} | {account.action_type} | "
                         f"{account.action_start} | {account.action_length} | {elapsed_hours}")
            if elapsed_hours >= account.action_length:
                await self.pop_action_time(account, elapsed_time)

    @check_action_times.before_loop
    async def before_check_action_times(self):
        await self.bot.wait_until_ready()
