import { mockMessage } from "@discord-bot/discord-mock"
import { useDbConnection } from "@discord-bot/mongo"
import PlayerUserModel from "@discord-bot/ol-fantasy-battle/dist/Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"

import { test, execute } from "./restoreBackup"

describe("Command: restoreBackup", () => {
  useDbConnection("Command_restoreBackup")

  describe(".test", () => {
    it("works", () => {
      expect(`!restoreBackup`).toMatch(test)
      expect(`!restore  Backup`).toMatch(test)
      expect(`!restore-Backup`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!restoreBackup --player jão`).toMatch(test)
      expect(test.exec(`!restoreBackup --player jão`)?.groups).toMatchObject({ flags: "--player jão" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!restoreBackup`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!restoreBackup --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!restoreBackup --player "Ragan"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })

      it(`complains if there is no file attachment`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!restoreBackup`
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        await playerUser.save()
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`There is no JSON backup file attached to your message. Send the backup file with the message "!restoreBackup"`)
      })
    })
  })
})