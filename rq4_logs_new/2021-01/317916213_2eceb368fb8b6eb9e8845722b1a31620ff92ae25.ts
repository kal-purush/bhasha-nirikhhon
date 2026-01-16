import { Discord } from "@discord-bot/create-client"
import logger from "./logger"

type Flags = { [key: string]: string | number | boolean | undefined }

const flagEntriesStr = (flags: Flags) => Object
  .entries(flags)
  .filter(([_, value]) => value !== undefined)
  .map(([flagName, value]) => `--${flagName}="${value}"`)
  .join(" ")

const getFlagsStr = (flags?: Flags) => !flags ? "" : ` with arguments: ${flagEntriesStr(flags)}`

export const logSuccess = (commandName: string, message: Discord.Message, flags?: Flags) =>
  logger.info(`FB: (Command) ${commandName}: user "${message.author.username}" called ${commandName}` + getFlagsStr(flags))

export const logFailure = (commandName: string, failureReason: string, message: Discord.Message, flags?: Flags) =>
  logger.info(`FB: (Command) ${commandName}: user "${message.author.username}" tried to call ${commandName}${getFlagsStr(flags)}, but ${failureReason}`)