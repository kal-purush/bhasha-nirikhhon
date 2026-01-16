import { mockMessage } from "@discord-bot/discord-mock"
import { useDbConnection } from "@discord-bot/mongo"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"

import { test, execute } from "./createBackup"

describe("Command: createBackup", () => {
  useDbConnection("Command_createBackup")

  describe(".test", () => {
    it("works", () => {
      expect(`!createBackup`).toMatch(test)
      expect(`!create  Backup`).toMatch(test)
      expect(`!create-Backup`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!createBackup --player jão`).toMatch(test)
      expect(test.exec(`!createBackup --player jão`)?.groups).toMatchObject({ flags: "--player jão" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!createBackup`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!createBackup --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!createBackup --player "Ragan"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        // TODO: maybe bother creating proper tests for this?
      })
    })
  })
})