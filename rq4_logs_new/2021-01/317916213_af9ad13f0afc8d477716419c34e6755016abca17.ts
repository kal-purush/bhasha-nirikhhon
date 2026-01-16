import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"

import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./setAtkAttribute"

describe("Command: listChars", () => {
  useDbConnection("Command_listChars")

  describe(".test", () => {
    it("works without args", () => {
      expect(`!set-atk-attb`).toMatch(test)
      expect(`!set-atk-atribute`).toMatch(test)
      expect(`!set-atk-attribute`).toMatch(test)
      expect(`!set-atkatb`).toMatch(test)
      expect(`!setattk-atb`).toMatch(test)
      expect(`!set-attack-atb`).toMatch(test)
      expect(`!setattack-atb`).toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!set-atk-attb  --player Jorge`).toMatch(test)
      expect(test.exec(`!set-atk-attb  --player Jorge`)?.groups).toMatchObject({ flags: "--player Jorge"})
      expect(`!set-atk-attb  --player=Jorge`).toMatch(test)
      expect(test.exec(`!set-atk-attb  --player=Jorge`)?.groups).toMatchObject({ flags: "--player=Jorge"})
      expect(`!set-atk-attb    --player "Jorge"`).toMatch(test)
      expect(test.exec(`!set-atk-attb    --player "Jorge"`)?.groups).toMatchObject({ flags: "--player \"Jorge\""})
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!set-atk-attb`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-atk-attb --player "OtherPlayer" --char "Test" --attb Might`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-atk-attb --char Kuff --attb Might`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!set-atk-attb --player "Ragan" --char "Kuff" --attb Might`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
      
      it(`complains if bad --atk-attb`, async () => {
        const [message] = mockPlayerMessage()
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        await playerUser.save()
        message.content = `!set-atk-attb --char "Kuff"  --attb Maight`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`value "Maight" isn't a valid attribute name`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-atk-attb --char Horu --attb Entropy`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        await playerUser.save()
        expect(playerUser?.activeChar.defaultAtkAttb).toBe("Might")

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(message.author.id)
        expect(playerUser2?.activeChar.defaultAtkAttb).toBe("Entropy")
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Horu's default attribute for attacks is now Entropy`)
      })

      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-atk-attb --char Kuff --attb Deception`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(message.author.id)
        expect(playerUser2?.characters[1].name).toBe("Kuff")
        expect(playerUser2?.characters[1].defaultAtkAttb).toBe("Deception")
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Kuff's default attribute for attacks is now Deception`)
      })

      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!set-atk-attb --char Ynit --player "testPlayer" --attb Agility`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Ynit" }))
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser("42069420")
        expect(playerUser2?.characters[1].name).toBe("Ynit")
        expect(playerUser2?.characters[1].defaultAtkAttb).toBe("Agility")
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Ynit's default attribute for attacks is now Agility`)})
    })

  })
})