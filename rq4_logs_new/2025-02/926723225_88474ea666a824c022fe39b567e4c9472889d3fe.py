import modio
import asyncio
import sqlite3
import time
import os
from discord.ext import commands

from functions.common import get_all_mods, get_latest_comment

# from dotenv import load_dotenv
#
# load_dotenv('data/server.env')
BOT_ACCESS_ROLE = int(os.getenv('BOT_ACCESS_ROLE'))
#
# async def get_all_mods(gameid, qmfilter):
#     offsetValue = 0
#     allMods = []
#     queryCount = 0
#     tagString = ''
#
#     while True:
#         queryCount += 1
#         if queryCount > 100:
#             print(f'Waiting 60 seconds to avoid rate-limiting.')
#             time.sleep(60)
#             queryCount = 0
#
#         modList = await gameid.async_get_mods(filters=qmfilter)
#         print(modList)
#         offsetValue += len(modList.results)
#         qmfilter.offset(offsetValue)
#
#         allMods.extend(modList.results)
#
#         if not len(modList.results):
#             return allMods


# async def main():
#     game = await get_game()
#     await get_all_mods(game)
#
# if __name__ == "__main__":
#     loop = asyncio.run(main())

class Database(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name='db_dungeons')
    @commands.has_any_role(BOT_ACCESS_ROLE)
    async def db_dungeons(self, ctx, option: str = f'no'):
        tagString = ''
        comment = ''
        attempts = completions = failures = wrTime = wrName = completionTime = completionAvg = 0

        con = sqlite3.connect(f'data/dungeon_database.db'.encode('utf-8'))
        cur = con.cursor()

        qmfilter = modio.objects.Filter()
        qmfilter.limit(100)
        qmfilter.sort("id", reverse=False)
        qmfilter.equals(tags=f'Dungeon')

        commentfilter = modio.Filter()
        commentfilter.limit(1)
        commentfilter.sort("id", reverse=True)

        modList, queryCount = await get_all_mods(self.bot.game, qmfilter)

        # print(len(modList.results), modList.results)

        for index, mod in enumerate(modList):
            print(f'Processing comment for mod # {index} of {len(modList)}...')
            if queryCount > 100:
                print(f'Waiting 60 seconds to avoid rate-limiting.')
                # time.sleep(60)
                queryCount = 0

            if 'comments' in option:
                comment = await get_latest_comment(mod, commentfilter)
                splitComment = comment.split(f'|')
                # account for comments before june 2024
                if len(splitComment) == 5:
                    attempts, completions, failures, wrTime, wrName = splitComment
                    completionTime = 0
                    completionAvg = 0
                elif len(splitComment) == 7:
                    attempts, completions, failures, wrTime, wrName, completionTime, completionAvg = splitComment
                queryCount += 1

            # print(mod.id, mod.name)
            # mod_detail = re.match(r'<Mod id=(.*?) name=(.*?) game_id=', str(mod_result))
            # mod = await game.async_get_mod(mod_detail.groups()[0])

            # modid = mod_detail.groups(0)
            # modname = str(mod_detail.groups(1))
            # print(mod.id, mod.name)
            # queryCount += 1
            # if queryCount > 60:
            #     print(f'Waiting 60 seconds to avoid rate-limiting.')
            #     time.sleep(60)
            #     queryCount = 0
            mod.name = mod.name.replace('"', '\"')
            mod.name = mod.name.replace('\'', '\'\'')
            mod.submitter.username = mod.submitter.username.replace('"', '\"')
            mod.submitter.username = mod.submitter.username.replace('\'', '\'\'')
            mod.summary = mod.summary.replace('"', '\"')
            mod.summary = mod.summary.replace('\'', '\'\'')
            for tag in mod.tags:
                tagString += f'{tag}, '
            # print(tagString)
            tagString.replace('"', '\"')
            tagString.replace('\'', '\'\'')
            if 'comments' in option:
                cur.execute(f"INSERT OR REPLACE INTO dungeons (id, name, creator_user_id, "
                            f"summary, link, tags, latest_comment, likes, attempts, completions, failures,"
                            f"worldrecordduration, worldrecordholder, completiontimecount, completiontimeaverage) "
                            f"values ({mod.id}, \'{mod.name}\', {mod.submitter.id}, "
                            f"\'{mod.summary}\', \'{mod.profile}\', \'{tagString[:-2]}\', \'{comment}\', "
                            f"{mod.stats.positive}, {attempts}, {completions}, {failures}, {wrTime}, "
                            f"{wrName}, {completionTime}, {completionAvg});")
            else:
                cur.execute(f"UPDATE dungeons set likes = {mod.stats.positive} where id = {mod.id});")

            con.commit()
            tagString = ''

        con.close()

        await ctx.reply(f'Updated the database!')

    @commands.command(name='db_users')
    @commands.has_any_role(BOT_ACCESS_ROLE)
    async def db_users(self, ctx):

        qmfilter = modio.objects.Filter()
        qmfilter.limit(100)
        qmfilter.sort("id", reverse=False)
        qmfilter.equals(tags=f'User')

        con = sqlite3.connect(f'data/dungeon_database.db'.encode('utf-8'))
        cur = con.cursor()

        modList = await get_all_mods(self.bot.game, qmfilter)
        print(len(modList))

        for mod in modList:
            mod.submitter.username = mod.submitter.username.replace('"', '\"')
            mod.submitter.username = mod.submitter.username.replace('\'', '\'\'')
            print(mod.id, mod.name)
            cur.execute(f"INSERT OR REPLACE INTO users (mod_id, user_id, username) "
                        f"values ({mod.id}, {int(mod.name)}, \'{mod.submitter.username}\');")

        con.commit()
        con.close()
        await ctx.reply(f'Updating the database!')



@commands.Cog.listener()
async def setup(bot):
    await bot.add_cog(Database(bot))