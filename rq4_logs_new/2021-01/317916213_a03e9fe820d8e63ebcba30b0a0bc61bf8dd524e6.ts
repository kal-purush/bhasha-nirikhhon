import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./setAttribute"

describe("Command: rollDmg", () => {
  useDbConnection("Command_rollDmg")

  describe(".test", () => {
    it("works", () => {
      expect(`!set Might`).toMatch(test)
      expect(`!setAttb    decep`).toMatch(test)
      expect(`!setAttribute    agi`).toMatch(test)
      expect(`!setarAtributo fORTitude`).toMatch(test)

      expect(`!setAttb`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!set Might --character Kuff`).toMatch(test)
      expect(`!setAttb    decep --character Kuff`).toMatch(test)
      expect(`!setAttribute    agi --character Kuff`).toMatch(test)
      expect(`!setarAtributo fORTitude --character Kuff`).toMatch(test)
      expect(test.exec(`!set Might --character Kuff`              )?.groups).toMatchObject({ attbNickname: "Might", flags: "--character Kuff" })
      expect(test.exec(`!setAttb    decep --character Kuff`       )?.groups).toMatchObject({ attbNickname: "decep", flags: "--character Kuff" })
      expect(test.exec(`!setAttribute    agi --character Kuff`    )?.groups).toMatchObject({ attbNickname: "agi", flags: "--character Kuff" })
      expect(test.exec(`!setarAtributo fORTitude --character Kuff`)?.groups).toMatchObject({ attbNickname: "fORTitude", flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!setAttb might`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })

      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })

    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!setAttb might --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })

      it(`complains if there is no --value or --bonus flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might`
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`This command needs at least a --value or --bonus flag. Try again with one of those, please (ex: !setAttb might --bonus 6)`)
      })
    })

    describe("happy paths", () => {
      it("works with --value", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might --value 3`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Might.value = 4
        playerUser.characters[0].attributes.Might.bonus = 1
        await playerUser.save()
        expect(playerUser.characters[0].attributes.Might.value).toBe(4)
        expect(playerUser.characters[0].attributes.Might.bonus).toBe(1)

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[0].attributes.Might.value).toBe(3)
        expect(playerUser2?.characters[0].attributes.Might.bonus).toBe(1)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Horu's Might value changed from 4(+1) to 3(+1)`)
      })

      it("works with --bonus", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might --bonus 2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Might.value = 4
        playerUser.characters[0].attributes.Might.bonus = 1
        await playerUser.save()
        expect(playerUser.characters[0].attributes.Might.value).toBe(4)
        expect(playerUser.characters[0].attributes.Might.bonus).toBe(1)

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[0].attributes.Might.value).toBe(4)
        expect(playerUser2?.characters[0].attributes.Might.bonus).toBe(2)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Horu's Might value changed from 4(+1) to 4(+2)`)
      })

      it("works with --value and --bonus", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb might --value 3 --bonus 2`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.characters[0].attributes.Might.value = 4
        playerUser.characters[0].attributes.Might.bonus = 1
        await playerUser.save()
        expect(playerUser.characters[0].attributes.Might.value).toBe(4)
        expect(playerUser.characters[0].attributes.Might.bonus).toBe(1)

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[0].attributes.Might.value).toBe(3)
        expect(playerUser2?.characters[0].attributes.Might.bonus).toBe(2)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Horu's Might value changed from 4(+1) to 3(+2)`)
      })
      
      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setAttb fort --char Kuff --value 2 --bonus -1`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[1].attributes.Fortitude.value = 4
        playerUser.characters[1].attributes.Fortitude.bonus = 1
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(playerUser2?.characters[1].attributes.Fortitude.value).toBe(2)
        expect(playerUser2?.characters[1].attributes.Fortitude.bonus).toBe(-1)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Kuff's Fortitude value changed from 4(+1) to 2(-1)`)
      })

      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!setAttb Agilidade --player "testPlayer" --char Kuff --value 0 --bonus 0`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Agility.value = 4
        playerUser.characters[0].attributes.Agility.bonus = 1
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(playerUser.userId)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Kuff's Agility value changed from 4(+1) to 0`)
        expect(playerUser2?.characters[0].attributes.Agility.value).toBe(0)
        expect(playerUser2?.characters[0].attributes.Agility.bonus).toBe(0)
      })
    })
  })
})