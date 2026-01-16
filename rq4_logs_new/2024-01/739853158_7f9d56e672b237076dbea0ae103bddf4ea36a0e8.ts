import {ContextMessageUpdate, Middleware} from "telegraf";
import {userRepository} from "../repository/user";
import {replyError} from "../utils/common";

export const createUser: Middleware<ContextMessageUpdate> = async (ctx) => {
  await ctx.reply('Добро пожаловать!\nВведите /help для ознакомления с возможностями бота!');
  try {
    await userRepository.createUser(Number(ctx.message?.from?.id), String(ctx.message!.from!.username))
  } catch (e) {
    // await ctx.replyWithMarkdown('```' + String(e) + '```');
    await replyError(ctx);
  }
}