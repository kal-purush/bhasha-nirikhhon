import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"
import { useDbConnection } from "@discord-bot/mongo"

import { test, execute } from "./rollInitiative"

describe("Command: rollInitiative", () => {
  useDbConnection("Command_rollInitiative")

  describe(".test", () => {
    it("works", () => {
      expect(`!init`).toMatch(test)
      expect(`!iniciativa`).toMatch(test)
      expect(`!rollinit`).toMatch(test)
      expect(`!roll iniciativa`).toMatch(test)
      expect(`!initiative`).toMatch(test)
      expect(`!initiative +2adv-1`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!init --player "Jorge"`).toMatch(test)
      expect(test.exec(`!init --player "Jorge"`)?.groups).toMatchObject({ flags: "--player \"Jorge\"" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!init`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!init --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!init --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!init --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })

    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!init`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = 4
        playerUser.characters[0].attributes.Agility.bonus = 0
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent.indexOf("**Horu**, rolling Initiative:\n")).not.toBe(-1)
        expect(messageSent).toMatch(/__\*\*1d20\+4\*\*__: \d+ \(\+4\) = -?\d+/)
      })
      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!init --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Agility.value = 3
        playerUser.characters[1].attributes.Agility.value = 2
        playerUser.characters[1].attributes.Agility.bonus = -3
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20-1\*\*__: \d+ \(-1\) = -?\d+/)
        expect(messageSent.indexOf("**Kuff**, rolling Initiative:\n")).not.toBe(-1)
      })
      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!init --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = -2
        playerUser.characters[0].attributes.Agility.bonus = -3
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20-5\*\*__: \d+ \(-5\) = -?\d+/)
        expect(messageSent.indexOf("**Horu**, rolling Initiative:\n")).not.toBe(-1)
      })
      it(`works with bonus argument`, async () => {
        const [message] = mockDmMessage()
        message.content = `!init +3`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = -3
        playerUser.characters[0].attributes.Agility.bonus = 5
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toMatch(/__\*\*1d20\+5\*\*__: \d+ \(\+5\) = -?\d+/)
        expect(messageSent.indexOf("**Horu**, rolling Initiative:\n")).not.toBe(-1)
      })
      it(`works with advantage`, async () => {
        const [message] = mockDmMessage()
        message.content = `!init -3adv+2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = 1
        playerUser.characters[0].attributes.Agility.bonus = 3
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toMatch(/__\*\*1d20\+1 adv\+2\*\*__: .+ \(\+1\) = -?\d+/)
        expect(messageSent.indexOf("**Horu**, rolling Initiative:\n")).not.toBe(-1)
      })
    })
  })
})