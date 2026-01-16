const config = require("../config");
const { command, isPrivate, getJson, sleep, tiny, getBuffer, styletext, listall } = require("../lib/");
const { Image } = require("node-webpmux");

command(
  {
    pattern: "take",
    fromMe: isPrivate,
    desc: "Change sticker data",
    type: "converter",
  },
  async (message, match, m) => {
    if (!message.reply_message && !message.reply_message.sticker)
      return await message.reply("_Reply to sticker_");
    let buff = await m.quoted.download();
    let [packname, author] = match.split(",");
    await message.sendMessage(
      buff,
      {
        packname: packname || config.STICKER_DATA.split(";")[0],
        author: author || config.STICKER_DATA.split(";")[1] },
      "sticker"
    );
  }
);