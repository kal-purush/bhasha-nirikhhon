import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./bio"

describe("Command: rollDmg", () => {
  useDbConnection("Command_rollDmg")

  describe(".test", () => {
    it("works", () => {
      expect(`!bio`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!bio --character Kuff`).toMatch(test)
      expect(test.exec(`!bio --character Kuff`)?.groups).toMatchObject({ flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!bio`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!bio --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!bio --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!bio --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!bio`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu", level: 3 }))
        playerUser.characters[0].attributes.Might.value = 4
        playerUser.characters[0].attributes.Might.bonus = -3
        playerUser.characters[0].attributes.Creation.bonus = 3
        playerUser.characters[0].updateMaxResources()
        playerUser.characters[0].hp.current = 11
        playerUser.characters[0].mp.current = 8

        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        
        expect(messageSent).toBe(
          "**Horu** (level 3)\n" +
          "\nHP:   **11/20**" +
          "\nMP:   **8/14**" +

          "\n\n**Physical**:" +
          "\n\tAgility: 0" + 
          "\n\tFortitude: 0" + 
          "\n\tMight: 4 (-3)" + 

          "\n\n**Mental**:" +
          "\n\tLearning: 0" +
          "\n\tLogic: 0" +
          "\n\tPerception: 0" +
          "\n\tWill: 0" +
          
          "\n\n**Social**:" +
          "\n\tDeception: 0" +
          "\n\tPersuasion: 0" +
          "\n\tPresence: 0" +
          
          "\n\n**Special**:" +
          "\n\tAlteration: 0" +
          "\n\tCreation: 0 (+3)" +
          "\n\tEnergy: 0" +
          "\n\tEntropy: 0" +
          "\n\tInfluence: 0" +
          "\n\tMovement: 0" +
          "\n\tPrescience: 0" +
          "\n\tProtection: 0"
        )
      })

      it("works with --char flag", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!bio --char Kuff`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.characters[0].attributes.Fortitude.value = 3 // horu
        playerUser.characters[1].attributes.Fortitude.value = 8 // kuff
        playerUser.characters[1].attributes.Might.value = 4
        playerUser.characters[1].attributes.Might.bonus = -3
        playerUser.characters[1].attributes.Creation.bonus = 3
        playerUser.characters[1].updateMaxResources()
        playerUser.characters[1].hp.current = 11
        playerUser.characters[1].mp.current = 8
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        
        expect(messageSent).toBe(
          "**Kuff** (level 1)\n" +
          "\nHP:   **11/32**" +
          "\nMP:   **8/10**" +

          "\n\n**Physical**:" +
          "\n\tAgility: 0" + 
          "\n\tFortitude: 8" + 
          "\n\tMight: 4 (-3)" + 

          "\n\n**Mental**:" +
          "\n\tLearning: 0" +
          "\n\tLogic: 0" +
          "\n\tPerception: 0" +
          "\n\tWill: 0" +
          
          "\n\n**Social**:" +
          "\n\tDeception: 0" +
          "\n\tPersuasion: 0" +
          "\n\tPresence: 0" +
          
          "\n\n**Special**:" +
          "\n\tAlteration: 0" +
          "\n\tCreation: 0 (+3)" +
          "\n\tEnergy: 0" +
          "\n\tEntropy: 0" +
          "\n\tInfluence: 0" +
          "\n\tMovement: 0" +
          "\n\tPrescience: 0" +
          "\n\tProtection: 0"
        )
      })
      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!bio --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Llat Drat'sab", level: 2 }))
        playerUser.characters[0].attributes.Fortitude.value = 3 // horu
        playerUser.characters[0].attributes.Fortitude.value = 8 // kuff
        playerUser.characters[0].attributes.Might.value = 4
        playerUser.characters[0].attributes.Might.bonus = -3
        playerUser.characters[0].attributes.Creation.bonus = 3
        playerUser.characters[0].updateMaxResources()
        playerUser.characters[0].hp.current = 11
        playerUser.characters[0].mp.current = 8
        await playerUser.save()

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
        expect(messageSent).toBe(
          "**Llat Drat'sab** (level 2)\n" +
          "\nHP:   **11/34**" +
          "\nMP:   **8/12**" +

          "\n\n**Physical**:" +
          "\n\tAgility: 0" + 
          "\n\tFortitude: 8" + 
          "\n\tMight: 4 (-3)" + 

          "\n\n**Mental**:" +
          "\n\tLearning: 0" +
          "\n\tLogic: 0" +
          "\n\tPerception: 0" +
          "\n\tWill: 0" +
          
          "\n\n**Social**:" +
          "\n\tDeception: 0" +
          "\n\tPersuasion: 0" +
          "\n\tPresence: 0" +
          
          "\n\n**Special**:" +
          "\n\tAlteration: 0" +
          "\n\tCreation: 0 (+3)" +
          "\n\tEnergy: 0" +
          "\n\tEntropy: 0" +
          "\n\tInfluence: 0" +
          "\n\tMovement: 0" +
          "\n\tPrescience: 0" +
          "\n\tProtection: 0"
        )
      })
    })
  })
})