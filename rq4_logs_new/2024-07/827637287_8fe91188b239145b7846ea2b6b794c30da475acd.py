import telebot
import json
from message import *


bot = telebot.TeleBot("5229400507:AAHGPiOyfIlliYuD2boZVlqRK5SwbcBi0OY")

import telebot
import json



# Owner ID (Replace with your Telegram user ID)
owner_id = 5470956337
owner_id2 = 5470956337

# Load banned users from JSON file
try:
    with open('banned_users.json', 'r') as f:
        banned_users = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    banned_users = {}

# Load safe users from JSON file
try:
    with open('safe_users.json', 'r') as f:
        data = f.read()
        if data.strip():
            safe_users = json.loads(data)
        else:
            safe_users = {}
except (FileNotFoundError, json.JSONDecodeError):
    safe_users = {}

# Function to check if user is owner of the bot
def is_owner(message):
    try:
        if message.chat.type == "private" and message.from_user.id == owner_id:
            return True

        if message.from_user.id == owner_id or message.from_user.id == owner_id2:
            return True

        chat_admins = bot.get_chat_administrators(message.chat.id)
        for admin in chat_admins:
            if admin.status == 'administrator' and admin.can_promote_members:
                if admin.user.id == message.from_user.id:
                    return True
        return False
    except Exception as e:
        print(f"Error in is_owner: {e}")
        return False

# Remove user from safe list
@bot.message_handler(commands=['remove_sf'])
def remove_safe_user(message):
    try:
        if is_owner(message):
            if len(message.text.split()) == 2:
                user_id = message.text.split()[1]
                if user_id in safe_users:
                    del safe_users[user_id]
                    with open('safe_users.json', 'w') as f:
                        json.dump(safe_users, f)
                    bot.reply_to(message, f"User with ID {user_id} has been removed from the safe list.")
                else:
                    bot.reply_to(message, f"User with ID {user_id} is not in the safe list.")
            else:
                bot.reply_to(message, "Please provide a user ID to remove from the safe list.")
        else:
            bot.reply_to(message, "❌ You do not have access to this command.")
    except Exception as e:
        print(f"Error in remove_safe_user: {e}")

# Remove user from scammer list
@bot.message_handler(commands=['remove_sc'])
def remove_scammer_user(message):
    try:
        if is_owner(message):
            if len(message.text.split()) == 2:
                user_id = message.text.split()[1]
                if user_id in banned_users:
                    del banned_users[user_id]
                    with open('banned_users.json', 'w') as f:
                        json.dump(banned_users, f)
                    bot.reply_to(message, f"User ID {user_id} has been removed from the scammer list.")
                else:
                    bot.reply_to(message, f"User ID {user_id} is not in the scammer list.")
            else:
                bot.reply_to(message, "Please provide a user ID to remove from the scammer list.")
        else:
            bot.reply_to(message, "❌ You do not have access to this command.")
    except Exception as e:
        print(f"Error in remove_scammer_user: {e}")

# Add user to safe list and remove from banned list if necessary
@bot.message_handler(commands=['add_sf'])
def add_safe_user(message):
    try:
        if is_owner(message):
            if len(message.text.split()) == 2:
                user_id = message.text.split()[1]
                username = message.reply_to_message.from_user.username if message.reply_to_message else None
                safe_users[user_id] = username

                # Remove from banned list if present
                if user_id in banned_users:
                    del banned_users[user_id]

                with open('safe_users.json', 'w') as f:
                    json.dump(safe_users, f)
                with open('banned_users.json', 'w') as f:
                    json.dump(banned_users, f)

                bot.reply_to(message, f"✅ User {user_id} was added to the SAFE LIST.")
            else:
                bot.reply_to(message, "Please provide a user ID to mark as safe.")
        else:
            bot.reply_to(message, "Only the owner of the bot can use the /add_sf command.")
    except Exception as e:
        print(f"Error in add_safe_user: {e}")
        bot.reply_to(message, "An error occurred while processing the safe command.")

# Add user to scammer list and remove from safe list if necessary
@bot.message_handler(commands=['add_sc'])
def add_scammer_user(message):
    try:
        if is_owner(message):
            if len(message.text.split()) == 2:
                user_id = message.text.split()[1]
                username = message.reply_to_message.from_user.username if message.reply_to_message else None
                banned_users[user_id] = username

                # Remove from safe list if present
                if user_id in safe_users:
                    del safe_users[user_id]

                with open('banned_users.json', 'w') as f:
                    json.dump(banned_users, f)
                with open('safe_users.json', 'w') as f:
                    json.dump(safe_users, f)

                bot.reply_to(message, f"❌ User was added on the SCAM list.\n\n👤 - [ {user_id} ]\n\n⚠️You were BANNED out of groups.")
            else:
                bot.reply_to(message, "Please provide a user ID to mark as scammer.")
        else:
            bot.reply_to(message, "Only the owner of the bot can use the /add_sc command.")
    except Exception as e:
        print(f"Error in add_scammer_user: {e}")
        bot.reply_to(message, "An error occurred while processing the scammer command.")

# Ask command
@bot.message_handler(commands=['ask'])
def ask_command(message):
    try:
        if len(message.text.split()) == 2:
            username_or_id = message.text.split()[1]
            for user_id, username in banned_users.items():
                if username_or_id == username or username_or_id == user_id:
                    bot.reply_to(message, f"❌ User was added to the SCAM list.\n\n👤 [ {username_or_id} ]\n\n⚠️ You are BANNED from the groups.")
                    return
            
            for user_id, username in safe_users.items():
                if username_or_id == username or username_or_id == user_id:
                    bot.reply_to(message, f'✅ This User is marked as a SAFE !\n\n👤 [{username_or_id}]\n\n⚠️ Look carefully if the Username is not written in the "Bio"')
                    return
            
            bot.reply_to(message, f"⚠️ User was not found in this database!\n\n👤 - [ {username_or_id} ] \n\n❌ We do not recommend to make deals with this User.")
    except Exception as e:
        print(f"Error in ask_command: {e}")
        bot.reply_to(message, "An error occurred while processing the ask command.")

# Start the bot
bot.polling()