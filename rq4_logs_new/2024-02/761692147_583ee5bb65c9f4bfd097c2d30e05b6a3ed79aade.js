const TelegramBot = require("node-telegram-bot-api"); // the telegram bot api library
const botToken = "6849664260:AAHAZOLoZpJVAcjPqwH31sXeQVG8k_ZEIGw"; // bot token getten from botFather
const bot = new TelegramBot(botToken, { polling: true }); // create the bot with polling parameter 
bot.onText(/\/start/, (msg) => { // Start command handler
  const chatId = msg.chat.id; // Refers to the unique identifier of the chat conversation you are responding to. 
  // Reply directly to the user who sent the message or Send messages to the entire group or channel
  const userId = msg.from.id; // Represents the unique identifier of the user who sent the message you are currently processing.
  // Send a personal message to the user: for private responses or follow-up actions. Store user-specific information
  const firstName = msg.from.first_name;
  const lastName = msg.from.last_name;
  let welcomeMessage = "Hello ";
  if (lastName) { welcomeMessage += firstName + " " + lastName; } 
  else { welcomeMessage += firstName; }
  bot.sendMessage(chatId, welcomeMessage);
  // Send user data to PHP endpoint
  console.log("yup");
  fetch("http://localhost:80/php/todoApp/todos.php", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `userId=${userId}&chatId=${chatId}&userName=${encodeURIComponent(firstName)}`
  });
  console.log("task sent");
});
bot.startPolling();