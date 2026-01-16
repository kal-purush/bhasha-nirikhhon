import { Discord } from "@discord-bot/create-client"

import { PlayerUserDocument } from "../Models/PlayerUser"

import { logFailure, Flags } from "./commandLog"

const getPlayerChar = (commandName: string, player: PlayerUserDocument, message: Discord.Message, flags: Flags, char: string = `${flags.char}`) => {

  const playerChar = player.getCharacter(char)

  // if character not found
  if (!playerChar) {
    logFailure(commandName, `given character "${char}" didn't correspond to one of their characters`, message, flags)
    message.channel.send(`Player ${player.username} doesn't have a character that matches "${char}". Try "!listChars" to see available characters`)
  }

  return playerChar
}

export default getPlayerChar