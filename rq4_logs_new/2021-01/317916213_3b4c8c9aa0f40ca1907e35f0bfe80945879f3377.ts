import { mockMessage } from "@discord-bot/discord-mock"
import { useDbConnection } from "@discord-bot/mongo"
import PcModel from "@discord-bot/ol-fantasy-battle/dist/Models/PlayerCharacter"
import PlayerUserModel from "@discord-bot/ol-fantasy-battle/dist/Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"

import { test, execute } from "./viewStats"

describe("Command: viewStats", () => {
  useDbConnection("Command_viewStats")

  describe(".test", () => {
    it("works", () => {
      expect(`!stats`).toMatch(test)
      expect(`!char`).toMatch(test)
      expect(`!viewChar`).toMatch(test)
      expect(`!personagem`).toMatch(test)
      expect(`!viewStats`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!stats --character Kuff`).toMatch(test)
      expect(test.exec(`!stats --character Kuff`)?.groups).toMatchObject({ flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!stats`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!stats --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!stats --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!stats --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!char`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu", level: 3 }))
        playerUser.characters[0].hp.current = 11
        playerUser.characters[0].mp.current = 8
        playerUser.characters[0].guard.value = 1
        playerUser.characters[0].guard.bonus = 1
        playerUser.characters[0].dodge.value = 2
        playerUser.characters[0].dodge.bonus = 0

        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        
        expect(messageSent).toBe(
          "**Horu** (level 3)\n" +
          "\nHP:   **11/14**" +
          "\nMP:   **8/14**" +
          "\nGuard: 1 (+1)   Dodge: 2"
        )
      })
    })
  })
})