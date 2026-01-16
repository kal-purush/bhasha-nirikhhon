import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./setActiveChar"

describe("Command: setActiveChar", () => {
  useDbConnection("Command_setActiveChar")

  describe(".test", () => {
    it("works", () => {
      expect("!set active").toMatch(test)
      expect("!set-active-char").toMatch(test)
      expect("!set-active Char").toMatch(test)
      expect("!setActiveChar").toMatch(test)
      expect("!setActiveCharacter").toMatch(test)
    })

    it("captures arguments", () => {
      expect("!setActiveChar   --char Test").toMatch(test)
      expect(test.exec("!setActiveChar   --char Test")?.groups).toMatchObject({ flags: "--char Test" })
      expect("!setActive char   --char Test").toMatch(test)
      expect(test.exec("!setActive char   --char Test")?.groups).toMatchObject({ flags: "--char Test" })
    })
  })
  
  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!set-active`
        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-active --player "OtherPlayer" --char "Test"`
        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })

    describe("dealing with bad args", () => {
      it(`complains if --char flag is missing`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-active-char`
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`\"**!set-active-char**\": --char flag is necessary. Please include it and try again`)
      })
      
      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!set-active-char --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })

      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-active-char --char "Kuff"`
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })
    })
    
    describe("happy paths", () => {
      
      it("works by default", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!set-active --char Mellot`

        const playerUser = PlayerUserModel.createUser({ username: message.author.username, userId: message.author.id })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Kuff" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Mellot" }))
        await playerUser.save()
        expect(playerUser.activeCharIndex).toBe(0)

        await execute(message, test.exec(message.content)!)

        const fetchedUser = await PlayerUserModel.getUser(message.author.id)
        expect(fetchedUser?.activeCharIndex).toBe(1)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! "Mellot" set as currently active character for ${message.author.username}`)
      })

      it(`works with --player flag`, async () => {
        const [message] = mockDmMessage()
        message.content = `!set-active --char Horu --player "Ragan"`

        const playerUser = PlayerUserModel.createUser({ userId: "420420", username: "Ragan" })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Ssaak" }))
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        await playerUser.save()
        expect(playerUser.activeCharIndex).toBe(0)
        expect(playerUser?.activeChar.name).toBe("Ssaak")

        await execute(message, test.exec(message.content)!)

        const fetchedPlayer = await PlayerUserModel.getUser("420420")
        expect(fetchedPlayer?.activeCharIndex).toBe(1)
        expect(fetchedPlayer?.activeChar.name).toBe("Horu")
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! "Horu" set as currently active character for ${playerUser.username}`)})
    })
  })

})