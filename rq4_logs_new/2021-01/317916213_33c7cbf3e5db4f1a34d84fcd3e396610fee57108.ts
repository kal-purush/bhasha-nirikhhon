import { mockMessage } from "@discord-bot/discord-mock"
import { useDbConnection } from "@discord-bot/mongo"
import PcModel from "@discord-bot/ol-fantasy-battle/dist/Models/PlayerCharacter"
import PlayerUserModel from "@discord-bot/ol-fantasy-battle/dist/Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"

import { test, execute } from "./longRest"

describe("Command: longRest", () => {
  useDbConnection("Command_longRest")

  describe(".test", () => {
    it("works", () => {
      expect(`!longRest`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!longRest --character Kuff`).toMatch(test)
      expect(test.exec(`!longRest --character Kuff`)?.groups).toMatchObject({ flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!longRest`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!longRest --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!longRest --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!longRest --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it("works", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!longRest`
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu", level: 7 }))
        playerUser.activeChar.hp.current = 5
        playerUser.activeChar.mp.current = 0
        playerUser.activeChar.attributes[playerUser.activeChar.mpDiceAttb].value = 14

        await playerUser.save()
        await execute(message, test.exec(message.content)!)
        const fetchedChar = await PlayerUserModel.getUser(playerUser.userId)
        const messageSent = (message.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]

        expect(message.channel.send).toBeCalledTimes(1)
        expect(messageSent).toMatch(/\*\*Horu\*\*:\n/)
        expect(messageSent).toMatch(/HP:\s+\*\*\d+\/\d+\*\* \(recovered \d+\)\n/)
        expect(messageSent).toMatch(/MP:\s+\*\*\d+\/\d+\*\* \(recovered \d+\)/)
        expect(fetchedChar?.activeChar.hp.current).toBeGreaterThan(5)
        expect(fetchedChar?.activeChar.mp.current).toBeGreaterThan(0)
      })
    })
  })
})