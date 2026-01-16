import logger from "../../Utils/logger"
import { Command, CommandListener, CommandProps, CommandState, DefaultCommand, MessageListener, RegexCommand } from "./types"

export const isDefaultCommand = <(cmd: Command) => cmd is DefaultCommand>((cmd) => {
  return cmd.test instanceof Function
})

export const isRegexCommand = <(cmd: Command) => cmd is RegexCommand>((cmd) => {
  return cmd.test instanceof RegExp
})


// function to create the master event listener for the bot
const createMasterMessageListener = (commandListeners: Map<string, CommandListener>): MessageListener => {
  return (message) => {
    // ignore if message from bot
    if (message.author.bot) return
    
    // else, iterate over all command listeners and execute the first that hits
    for (const commandName of Array.from(commandListeners.keys())) {
      const listener = commandListeners.get(commandName)
      // if no map hit, go to next (shouldn't ever happen, but ok)
      if (!listener) continue
      // if passed on command test, execute command and return
      if (listener.test(message)) {
        logger.info(`create-client: CommandClient: executing command: "${commandName}"`)
        return listener.execute(message)
      }
    }
    return
  }
}

export const resetMasterMessageListener = ({ discordClient }: CommandProps, state: CommandState) => {
  // delete old master message listener if it existed
  if (state.discordMasterMessageListener) {
    discordClient.removeListener("message", state.discordMasterMessageListener)
  }

  // create and add new message event listener
  state.discordMasterMessageListener = createMasterMessageListener(state.commandListeners)
  discordClient.on("message", (...props) => {
    state.discordMasterMessageListener!(...props)
  })
}