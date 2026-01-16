import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./rollAttribute"

describe("Command: setAtkAttribute", () => {
  useDbConnection("Command_setAtkAttribute")

  describe(".test", () => {
    it("works", () => {
      expect(`!Might`).toMatch(test)
      expect(`!agi`).toMatch(test)
      expect(`!decep`).toMatch(test)
      expect(`!learn`).toMatch(test)
      expect(`!fORTitude`).toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!fORTitude`).toMatch(test)
      expect(test.exec(`!fORTitude`)?.groups).toMatchObject({ attbNickname: "fORTitude" })
      expect(`!mig --player=Jorge`).toMatch(test)
      expect(test.exec(`!mig --player=Jorge`)?.groups).toMatchObject({ attbNickname: "mig", flags: "--player=Jorge"})
      expect(`!learn  --player "Jorge"`).toMatch(test)
      expect(test.exec(`!learn  --player "Jorge"`)?.groups).toMatchObject({ attbNickname: "learn", flags: "--player \"Jorge\""})
    })
  })

  describe(".execute", () => {
    
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!fort`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!fort --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!pers --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!pers --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!agility`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = 4
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent.indexOf("**Horu**, rolling Agility:\n")).not.toBe(-1)
        expect(messageSent).toMatch(/__\*\*1d20\+8\*\*__: \d+ \(\+8\) = \d+/)
      })

      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!fort --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Fortitude.value = 3
        playerUser.characters[1].attributes.Fortitude.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\+4\*\*__: \d+ \(\+4\) = \d+/)
        expect(messageSent.indexOf("**Kuff**, rolling Fortitude:\n")).not.toBe(-1)
      })
      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!agility --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\*\*__: \d+$/)
        expect(messageSent.indexOf("**Horu**, rolling Agility:\n")).not.toBe(-1)
      })
      it(`works with bonus argument`, async () => {
        const [message] = mockDmMessage()
        message.content = `!agility +3`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Fortitude.value = 1
        playerUser.characters[0].attributes.Fortitude.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toMatch(/__\*\*1d20\+12\*\*__: \d+ \(\+12\) = \d+/)
        expect(messageSent.indexOf("**Horu**, rolling Agility:\n")).not.toBe(-1)
      })
      it(`works with advantage`, async () => {
        const [message] = mockDmMessage()
        message.content = `!agility -7adv+2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Fortitude.value = 1
        playerUser.characters[0].attributes.Fortitude.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(false).toBe(true)
      })
    })
  })
})