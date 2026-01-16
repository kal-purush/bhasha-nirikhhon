import logging
import os
import asyncio
from discord.ext import commands
import discord

class AdminCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name='refresh')
    async def refresh_command(self, context):
        """Reloads all cogs dynamically."""
        await self.reload_cogs()
        await context.send("Refreshed!")

    async def reload_cogs(self):
        logging.info("🔄 Reloading all cogs...")
        cogs = [f"cogs.{filename[:-3]}" for filename in os.listdir("./cogs") if filename.endswith(".py")]
        for cog in cogs:
            await self.bot.reload_extension(cog)
            logging.info(f"✅ Reloaded {cog}")

    @commands.command(name='purge_chat')
    @commands.has_permissions(manage_messages=True)
    async def purge_chat(self, ctx, chat_id: int = None):
        """Deletes all messages in a given chat based on chat ID while handling rate limits."""
        channel = self.bot.get_channel(chat_id) if chat_id else ctx.channel
        if not channel:
            await ctx.send("Invalid chat ID or channel not found.")
            return

        def is_not_pinned(message):
            return not message.pinned

        total_deleted = 0
        delay = 1  # Start with a 1-second delay
        while True:
            try:
                deleted = await channel.purge(limit=50, check=is_not_pinned)
                total_deleted += len(deleted)
                if len(deleted) < 50:
                    break  # Stop when fewer than 50 messages are deleted
                await asyncio.sleep(delay)  # Apply delay to avoid hitting rate limits
                delay = min(delay * 2, 10)  # Exponential backoff up to 10 seconds
            except discord.HTTPException as e:
                logging.warning(f"Rate limited or error: {e}")
                await asyncio.sleep(10)  # If hit by rate limit, wait longer
                continue
        
        await ctx.send(f"Deleted {total_deleted} messages in <#{channel.id}>.")

async def setup(bot):
    await bot.add_cog(AdminCog(bot))