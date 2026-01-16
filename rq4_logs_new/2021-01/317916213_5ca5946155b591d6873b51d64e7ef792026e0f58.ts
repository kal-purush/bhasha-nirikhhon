import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"
import { useDbConnection } from "@discord-bot/mongo"

import { test, execute } from "./setLevel"

describe("Command: setLevel", () => {
  useDbConnection("Command_setLevel")

  describe(".test", () => {
    it("works", () => {
      expect(`!set level 2`).toMatch(test)
      expect(`!setlv 1`).toMatch(test)
      expect(`!setnivel    5`).toMatch(test)
      expect(`!setarlevel  12`).toMatch(test)

      expect(`!setLvl`).not.toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!setLvl 2 --character Kuff`).toMatch(test)
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!setLevel 2`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })

      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setLevel 2 --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })

    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setLevel 2 --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!setLevel 2 --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })

    describe("happy paths", () => {
      it('works by default', async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setLvl 2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu", level: 1 }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[0].level).toBe(2)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Horu's Level changed from 1 to 2`)
      })
      
      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setLvl 2 --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu", level: 1 }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff", level: 1 }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[1].level).toBe(2)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Kuff's Level changed from 1 to 2`)
      })

      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!setLvl 12 --player "testPlayer" --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[0].level).toBe(12)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Kuff's Level changed from 1 to 12`)
      })
    })
  })
})