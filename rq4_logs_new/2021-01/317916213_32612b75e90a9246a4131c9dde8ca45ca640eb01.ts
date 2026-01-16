import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./rollDmg"

describe("Command: rollDmg", () => {
  useDbConnection("Command_rollDmg")

  describe(".test", () => {
    it("works", () => {
      expect(`!dmg`).toMatch(test)
      expect(`!dmg Might`).toMatch(test)
      expect(`!dmg    decep`).toMatch(test)
      expect(`!damage    agi`).toMatch(test)
      expect(`!rolldamage fORTitude`).toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!dmg --character Kuff`).toMatch(test)
      expect(`!dmg Might --character Kuff`).toMatch(test)
      expect(`!dmg    decep --character Kuff`).toMatch(test)
      expect(`!damage    agi --character Kuff`).toMatch(test)
      expect(`!rolldamage fORTitude --character Kuff`).toMatch(test)
      expect(test.exec(`!dmg --character Kuff`                  )?.groups).toMatchObject({ attbNickname: undefined, flags: "--character Kuff" })
      expect(test.exec(`!dmg Might --character Kuff`            )?.groups).toMatchObject({ attbNickname: "Might", flags: "--character Kuff" })
      expect(test.exec(`!dmg    decep --character Kuff`         )?.groups).toMatchObject({ attbNickname: "decep", flags: "--character Kuff" })
      expect(test.exec(`!damage    agi --character Kuff`        )?.groups).toMatchObject({ attbNickname: "agi", flags: "--character Kuff" })
      expect(test.exec(`!rolldamage fORTitude --character Kuff` )?.groups).toMatchObject({ attbNickname: "fORTitude", flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!dmg`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!dmg dmg --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!dmg --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!dmg --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!dmg`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Might.value = 4
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toEqual(expect.stringContaining("**Horu**, rolling damage for Might:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**1d10!**__:"))
      })

      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!dmg fort --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Fortitude.value = 3 // horu
        playerUser.characters[1].attributes.Fortitude.value = 8 // kuff
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toEqual(expect.stringContaining("**Kuff**, rolling damage for Fortitude:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**3d8!**__:"))
      })
      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!dmg Agilidade --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        // expect roll ro match formatting (and correct +8 bonus to show up)
        expect(messageSent).toEqual(expect.stringContaining("**Horu**, rolling damage for Agility:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**1d2!**__:"))
      })
      it(`extracts bonus`, async () => {
        const [message] = mockDmMessage()
        message.content = `!dmg entropy +3`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Entropy.bonus = 2
        playerUser.characters[0].attributes.Entropy.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toEqual(expect.stringContaining("**Horu**, rolling damage for Entropy:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**2d10!**__:"))
      })
      it(`extracts advantage`, async () => {
        const [message] = mockDmMessage()
        message.content = `!dmg +3adv+2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Ynit" }))
        playerUser.characters[0].attributes.Might.bonus = -1
        playerUser.characters[0].attributes.Might.value = 2
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toEqual(expect.stringContaining("**Ynit**, rolling damage for Might:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**1d10! adv+2**__:"))
      })
      it(`extracts explosion`, async () => {
        const [message] = mockDmMessage()
        message.content = `!dmg energy !! +3dis+2 --char allor`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Allor" }))
        playerUser.characters[1].attributes.Energy.bonus = 5
        playerUser.characters[1].attributes.Energy.value = 3
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toEqual(expect.stringContaining("**Allor**, rolling damage for Energy:\n"))
        expect(messageSent).toEqual(expect.stringContaining("__**8d4!! dis-2**__:"))
      })
    })
  })
})