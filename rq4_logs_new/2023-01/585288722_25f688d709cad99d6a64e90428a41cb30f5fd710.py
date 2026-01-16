import asyncio
import os

import discord
from discord.ext import commands
import asyncio
from scraper import Scraper
from captcha import Solver
from util import get_proxy_and_account, check_domain
from mail import connect_email, search_email, close_email
import env
import time


def main():
    # initialize discord bot
    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(intents=intents, command_prefix="!")

    # initialize solver
    solver = Solver(env.CAPTCHA_KEY)
    # initialize scraper
    scraper = Scraper(headless=False)

    @bot.event
    async def on_ready():
        guild_count = 0
        for guild in bot.guilds:
            print(f"- {guild.id} (name: {guild.name})")
            guild_count += 1
        print("BOT is in " + str(guild_count) + " guilds.")

    @bot.command()
    async def chegg(ctx, arg=None):
        if str(ctx.channel) not in env.ALLOWED_CHANNELS:
            return
        if not arg:
            await ctx.channel.send(f'{ctx.author.mention} Please send a link after the !chegg command.')
            return
        if not check_domain(link=ctx, domain="https://www.chegg.com"):
            await ctx.channel.send(f'{ctx.author.mention} Please only use chegg links.')
            return
        try:
            proxy, account = get_proxy_and_account()
            scraper.open_homeworkify(proxy=proxy, link=arg)
            captcha_path = scraper.get_captcha(account[0])
            result = solver.solve(captcha_path)
            scraper.submit_captcha(result)
            os.remove(captcha_path)

            # captcha retry sequence
            await asyncio.sleep(0.5)
            fail_count = 0
            while scraper.captcha_has_failed():
                fail_count += 1
                if fail_count > 2:
                    raise Exception("custom")
                retry_captcha_path = scraper.retry_get_captcha()
                result = solver.solve(retry_captcha_path)
                scraper.submit_captcha(result)
                os.remove(retry_captcha_path)
                await asyncio.sleep(0.5)

            mail = connect_email(account[0], account[1])
            link = search_email(mail)
            close_email(mail)

            scraper.open_answer_link(link)
            result_path = scraper.get_full_screenshot()
            with open(result_path, 'rb') as fh:
                file = discord.File(fh)
                # Send the document as a file attachment
                await ctx.channel.send(f'{ctx.author.mention} Here is the answer:', file=file)
                os.remove(result_path)

        except commands.errors.MissingRequiredArgument:
            await ctx.channel.send(f'{ctx.author.mention} Please send a link after the !chegg command.')
        except Exception as e:
            print(e)
            await ctx.channel.send(f'{ctx.author.mention} An error has occurred, please create a ticket.')

    bot.run(env.DISCORD_TOKEN)


def test():
    scraper = Scraper(headless=False)
    solver = Solver(env.CAPTCHA_KEY)
    proxy = env.PROXY_LIST[0]
    account = env.ACCOUNT_LIST[0]
    link = "https://www.chegg.com/homework-help/questions-and-answers/supercooling-observed-experiment-difference-would-see-solution-stirred-q29004107"
    print("opening homeworkify")
    scraper.open_homeworkify(proxy=proxy, link=link)
    print("getting captcha")
    captcha_path = scraper.get_captcha(account[0])
    print("solving")

    result = solver.solve(captcha_path)
    print(f"result: {result}")

    print("submitting captcha")
    scraper.submit_captcha(result)
    os.remove(captcha_path)
    time.sleep(0.5)

    fail_count = 0
    while scraper.captcha_has_failed():
        fail_count += 1
        if fail_count > 2:
            raise Exception("custom")
        print("captcha has failed")
        retry_captcha_path = scraper.retry_get_captcha()
        result = solver.solve(retry_captcha_path)
        scraper.submit_captcha(result)
        os.remove(retry_captcha_path)
        time.sleep(0.5)

    print("opening email")
    mail = connect_email(account[0], account[1])
    print("getting link")
    link = search_email(mail)
    print(link)
    close_email(mail)

    print("opening in 2nd webdriver")
    scraper.open_answer_link(link)
    scraper.get_full_screenshot()
    input("(Enter) to end process")


if __name__ == "__main__":
    main()
    # test()
    # DONE 1 have list of proxies and list of emails, start with list of 1 and 1
    # DONE 2 randomly choose 1 account and proxy to make request on homeworkify
    # DONE 3 use headless browser to make requests and save screenshot of image
    # DONE 4 make anti captcha sequence and error handling based on API docs
    # DONE 5 make email handling to grab link within same browser session (parse email and look for link)
    # DONE 6 take screenshot of resulting page (ALL IN HEADLESS MODE)
    # DONE 7 add discord bot implementation
    # TODO 8 figure out hosting and buildpacks for hosting (pebblehost or heroku)
    # TODO 9 deal with ApiException
    # TODO 10 grab html file to grab images resulting from there

