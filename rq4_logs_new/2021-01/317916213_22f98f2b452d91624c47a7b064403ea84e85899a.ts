
import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"

import { mockDmMessage, mockPlayerMessage } from "../Utils/mockMessage"
import { useDbConnection } from "../Utils/mongoTest"

import { test, execute } from "./deleteCharacter"

describe("Command: deleteCharacter", () => {
  useDbConnection("Command_deleteCharacter")

  describe(".test", () => {
    it("works", () => {
      expect("!delete-char").toMatch(test)
      expect("!delete-character").toMatch(test)
      expect("!deletechar").toMatch(test)
      expect("!Delete Character").toMatch(test)
      expect("!delete     char").toMatch(test)
    })

    it("captures arguments", () => {
      expect("!delete-char --char Test").toMatch(test)
      expect(test.exec("!delete-char --char Test")?.groups).toMatchObject({ flags: "--char Test" })
    })
  })
    
  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!delete-char`

        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!delete-char --player "OtherPlayer" --char "Test"`
        
        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })

    describe("dealing with bad args", () => {
      it(`complains if --char flag is missing`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!delete-char`

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`\"**!delete-char**\": --char flag is necessary. Please include it and try again`)
      })

      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!delete-char --char Kuff`

        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!deleteChar --player "Ragan" --char "Kuff"`
        
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!delete-char --char Horu`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        await playerUser.save()
        expect(playerUser?.characters.length).toBe(2)

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser(message.author.id)
        expect(playerUser2?.characters.length).toBe(2)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Character "Horu" created for ${message.author.username}`)
      })

      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!delete-char --char Horu --player "testPlayer"`

        const playerUser = PlayerUserModel.createUser({ userId: "42069420", username: "testPlayer" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Ynit" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        await playerUser.save()
        expect(playerUser?.characters.length).toBe(3)

        await execute(message, test.exec(message.content)!)

        const playerUser2 = await PlayerUserModel.getUser("42069420")
        expect(playerUser2?.characters.length).toBe(0)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! Character "Horu" created for testPlayer`)})
    })
  })
})