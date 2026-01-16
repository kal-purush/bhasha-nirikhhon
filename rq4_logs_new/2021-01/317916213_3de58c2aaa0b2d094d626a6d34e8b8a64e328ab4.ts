import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"
import { useDbConnection } from "@discord-bot/mongo"

import { test, execute } from "./rollAtk"

describe("Command: rollAtk", () => {
  useDbConnection("Command_rollAtk")

  describe(".test", () => {
    it("works", () => {
      expect(`!atk`).toMatch(test)
      expect(`!ataque`).toMatch(test)
      expect(`!rollatk`).toMatch(test)
      expect(`!atk learn`).toMatch(test)
      // console.log(test)
      expect(test.exec(`!rollAtk learn`)?.groups).toMatchObject({ attbNickname: "learn" })
      expect(`!rollAtk fORTitude`).toMatch(test)
      expect(test.exec(`!rollAtk fORTitude`)?.groups).toMatchObject({ attbNickname: "fORTitude" })
    })
    it(`captures flags`, () => {
      expect(`!atk fort --player jão`).toMatch(test)
      expect(test.exec(`!atk fort --player jão`)?.groups).toMatchObject({ attbNickname: "fort", flags: "--player jão" })
      expect(`!atk --player=Jorge`).toMatch(test)
      expect(test.exec(`!atk --player=Jorge`)?.groups).toMatchObject({ flags: "--player=Jorge"})
    })
  })

  describe(".execute", () => {
    
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!atk`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!atk --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Agility.value = 3
        playerUser.characters[0].attributes.Agility.bonus = 1
        playerUser.characters[0].defaultAtkAttb = "Agility"
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent.indexOf("**Horu**, attacking with Agility:\n")).not.toBe(-1)
        expect(messageSent).toMatch(/__\*\*1d20\+4\*\*__: \d+ \(\+4\) = \d+/)
      })
      it("works with attribute argument", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk Entropy`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Entropy.value = 3
        playerUser.characters[0].attributes.Entropy.bonus = 1
        playerUser.characters[0].defaultAtkAttb = "Agility"
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent.indexOf("**Horu**, attacking with Entropy:\n")).not.toBe(-1)
        expect(messageSent).toMatch(/__\*\*1d20\+4\*\*__: \d+ \(\+4\) = \d+/)
      })
      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Might.value = 3
        playerUser.characters[1].attributes.Might.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\+2\*\*__: \d+ \(\+2\) = \d+/)
        expect(messageSent.indexOf("**Kuff**, attacking with Might:\n")).not.toBe(-1)
      })
      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!atk --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.activeChar.attributes.Agility.value = 3
        playerUser.activeChar.defaultAtkAttb = "Agility"

        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\+3\*\*__: \d+ \(\+3\) = \d+/)
        expect(messageSent.indexOf("**Horu**, attacking with Agility:\n")).not.toBe(-1)
      })
      it(`works with bonus argument`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!atk Ener +2 --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Might.value = 3
        playerUser.characters[1].attributes.Energy.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\+4\*\*__: \d+ \(\+4\) = \d+/)
        expect(messageSent.indexOf("**Kuff**, attacking with Energy:\n")).not.toBe(-1)
      })
      it(`works with advantage`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!rola atk crea -4   dis + 2 --char Ynit`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Ynit" }))
        playerUser.characters[0].attributes.Might.value = 3
        playerUser.characters[1].attributes.Creation.value = 6
        playerUser.characters[1].attributes.Creation.bonus = 3
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toMatch(/__\*\*1d20\+5 dis-2\*\*__:/)
        expect(messageSent.indexOf("**Ynit**, attacking with Creation:\n")).not.toBe(-1)
      })
    })
  })
})