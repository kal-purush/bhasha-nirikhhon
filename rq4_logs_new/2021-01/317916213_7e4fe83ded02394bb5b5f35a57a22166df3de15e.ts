import { Command, RegexCommand } from "@discord-bot/create-client"
import { capture, concat, nonCapture, optionalSpace, signedInteger } from "@discord-bot/regex"

import { hpRegex, mpRegex } from "../Models/PcResource/helpers"

import parseFlags, { FlagsObject } from "../Utils/parseArgs"
import rejectIfNotPlayerOrDm from "../Utils/rejectIfNotPlayerOrDm"
import { commandWithFlags } from "../Utils/regex"
import { getPlayerUser } from "../Utils/getUser"
import getPlayerChar from "../Utils/getPlayerChar"

export const test: RegexCommand.test = commandWithFlags(
  concat(
    capture("hp", hpRegex),
    optionalSpace,
    nonCapture(signedInteger("posHp", "negHp"))
  ),
  concat(
    capture("mp", mpRegex),
    optionalSpace,
    nonCapture(signedInteger("posMp", "negMp"))
  )
)

const changedResource = (value: number) => 
  value > 0 ? ` (+${value})` :
  value < 0 ? ` (${value})` :
  ``

export const execute: RegexCommand.execute = async (message, regexResult) => {
  
  // if user isn't admin or Player or DM, ignore
  if (rejectIfNotPlayerOrDm(message)) return

  // parse arguments
  const flagsObject: FlagsObject<{ player: string, char: string }> = {
    player: { type: "string", optional: true },
    char: { type: "string", optional: true },
  }
  const flags = parseFlags("!changeResource", flagsObject, regexResult?.groups?.flags, message)
  if (flags === null) return
  
  const { player } = await getPlayerUser("!changeResource", message, flags.player)
  if (player === null) return

  const character = getPlayerChar("!changeResource", player, message, flags)
  if (!character) return

  const changedHp =
    regexResult.groups?.posHp ? +Number(regexResult.groups?.posHp) :
    regexResult.groups?.negHp ? -Number(regexResult.groups?.negHp) :
    0
  const changedMp =
    regexResult.groups?.posMp ? +Number(regexResult.groups?.posMp) :
    regexResult.groups?.negMp ? -Number(regexResult.groups?.negMp) :
    0

  // don't allow value to go above max
  character.hp.current = Math.min(character.hp.current + changedHp, character.hp.max)
  character.mp.current = Math.min(character.mp.current + changedMp, character.mp.max)
  await player.save()

  message.channel.send(
    `**${character.name}**:`
    + `\nHP: ${character.hp.current}/${character.hp.max}${changedResource(changedHp)}`
    + `\nMP: ${character.mp.current}/${character.mp.max}${changedResource(changedMp)}`
  )
  return
}

export const changeResource: Command = {
  id: "Fantasy Battle: changeResource",
  test,
  execute,
}

export default changeResource