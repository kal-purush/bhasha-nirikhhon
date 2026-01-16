import { Discord } from "@discord-bot/create-client"

import PlayerUserModel, { PlayerUserDocument } from "../Models/PlayerUser"

import { isPlayer } from "./userPermissions"
import logger from "./logger"

export type getPlayerUser = (commandName: string, message: Discord.Message, playerFlag?: string) => Promise<{ player: PlayerUserDocument | null, fromFlags: boolean }>
export const getPlayerUser: getPlayerUser = async (commandName, message, playerFlag) => {
  const nullReturn = { player: null, fromFlags: false }

  // if bad permissions with flag --player
  if (playerFlag && isPlayer(message.member)) {
    logger.info(`FB: (Command) ${commandName}: user "${message.author.username}" tried to ${commandName} with --player flag, but they aren't a DM`)
    message.channel.send(`only DM's can use this command with the --player flag`)
    return nullReturn
  }

  // create of fetch the player user instance (if --player flag, fetch from flags)
  const player = playerFlag
    ? await PlayerUserModel.findOne({ username: playerFlag })
    : await PlayerUserModel.fromAuthor(message.author)

  // if user doesn't exist
  if (!player) {
    logger.info(`FB: (Command) ${commandName}: DM "${message.author.username}" tried to ${commandName} with "--player=${playerFlag}", but ${playerFlag} isn't a registered Player in the Database`)
    message.channel.send(`player "${playerFlag}" doesn't exist in my database. are you sure you typed their name correctly?`)
    return nullReturn
  }

  return { player, fromFlags: !!playerFlag }
}