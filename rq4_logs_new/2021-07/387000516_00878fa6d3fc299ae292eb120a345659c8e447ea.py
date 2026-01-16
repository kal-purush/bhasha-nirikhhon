import asyncio
from config import SUDO_USERS
from helpers.filters import command
from pyrogram import Client, filters
from helpers.decorators import errors

@Client.on_message(command("broadcast") & filters.user(SUDO_USERS) & ~filters.edited)
@errors
async def broadcast_message(_, message):
    if len(message.command) < 2:
        return await message.reply_text(
            "**Usage**:\n/broadcast (message)"
        )
    sleep_time = 3
    text = message.text.split(None, 1)[1]
    sent = 0
    schats = await get_served_chats()
    chats = [int(chat["chat_id"]) for chat in schats]
    m = await message.reply_text(
        f"Broadcast in progress, will take {len(chats) * sleep_time} seconds."
    )
    for i in chats:
        try:
            await Client.send_message(i, text=text)
            await asyncio.sleep(sleep_time)
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(int(e.x))
        except Exception:
            pass
    await m.edit(f"**Broadcasted Message In {sent} Chats.**")