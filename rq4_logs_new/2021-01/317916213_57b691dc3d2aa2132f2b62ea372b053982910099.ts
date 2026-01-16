import { Discord } from "@discord-bot/create-client"
import { isAttributeName } from "Models/PlayerCharacter/helpers"
import { AttributeName } from "Models/PlayerCharacter/types"
import { logFailure, Flags } from "./commandLog"

const validateAttributeName = (commandName: string, message: Discord.Message, flags: Flags, attributeFlagName: string): AttributeName | undefined | null => {
  const attribute = flags[attributeFlagName]

  if (attribute !== undefined && !isAttributeName(attribute)) {
    logFailure(commandName, `value "${attribute}" for --${attributeFlagName} flag is not a valid attribute name`, message, flags)
    message.channel.send(`value "${attribute}" isn't a valid attribute name`)
    return null
  }

  return attribute
}

export default validateAttributeName