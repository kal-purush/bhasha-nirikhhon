# need change destination_group_id = -1002115046888 | ultroidxTeam.t.me

from pyrogram import Client, filters
from DAXXMUSIC import app
from config import OWNER_ID, BOT_USERNAME
from pyrogram.types import Message


@app.on_message(filters.command(["post"], prefixes=["/", "."]) & filters.user(OWNER_ID))
async def copy_messages(_, message):

    if messdestination_group_id = --1002115046888age.reply_to_message:
      
        destination_group_id = -1002115046888
 

        
        await message.reply_to_message.copy(destination_group_id)
        await message.reply("ᴘᴏsᴛ sᴜᴄᴄᴇssғᴜʟ ᴅᴏɴᴇ ")