import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"
import { useDbConnection } from "@discord-bot/mongo"

import { test, execute } from "./takeDamage"

describe("Command: takeDamage", () => {
  useDbConnection("Command_takeDamage")

  describe(".test", () => {
    it("works", () => {
      expect(`!tkdmg 10`).toMatch(test)
      expect(`!take dmg 10`).toMatch(test)
      expect(`!tomar dmg 10`).toMatch(test)
      expect(`!tak damage 10`).toMatch(test)
      expect(`!tomedamage 10`).toMatch(test)
      expect(`!take dmg`).not.toMatch(test)
      expect("!wrong").not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!tkdmg 69 --character Kuff`).toMatch(test)
      expect(`!take dmg 69 --character Kuff`).toMatch(test)
      expect(`!tomar dmg 69 --character Kuff`).toMatch(test)
      expect(`!tak damage 69 --character Kuff`).toMatch(test)
      expect(`!tomedamage 69 --character Kuff`).toMatch(test)
      expect(test.exec(`!tkdmg 69 --character Kuff`      )?.groups).toMatchObject({ flags: "--character Kuff" })
      expect(test.exec(`!take dmg 69 --character Kuff`   )?.groups).toMatchObject({ flags: "--character Kuff" })
      expect(test.exec(`!tomar dmg 69 --character Kuff`  )?.groups).toMatchObject({ flags: "--character Kuff" })
      expect(test.exec(`!tak damage 69 --character Kuff` )?.groups).toMatchObject({ flags: "--character Kuff" })
      expect(test.exec(`!tomedamage 69 --character Kuff` )?.groups).toMatchObject({ flags: "--character Kuff" })
    })
  })

  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!tkdmg 8`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!tkdmg 8 --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!tkdmg 8 --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!tkdmg 8 --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it('works when there is no damage mitigation (dodge 0, guard 0)', async () => {
        const [message] = mockPlayerMessage()
        message.content = `!tkdmg 8`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        expect(playerUser.activeChar.hp.max).toBe(10)
        expect(playerUser.activeChar.hp.current).toBe(10)
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        const fetcherPlayer = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetcherPlayer?.activeChar.hp.max).toBe(10)
        expect(fetcherPlayer?.activeChar.hp.current).toBe(2)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(""
        + `Damage taken: 8`
        + `\n**Horu**`
        + `\nHP:   **2/10**`
        )
      })
      it('works when there is damage mitigation (dodge > 0 or guard > 0)', async () => {
        const [message] = mockPlayerMessage()
        message.content = `!tkdmg 24`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.activeChar.guard.bonus = 3
        playerUser.activeChar.dodge.bonus = 15
        
        const damageTaken = playerUser.activeChar.takeDamage(24)
        playerUser.activeChar.hp.current += damageTaken

        expect(playerUser.activeChar.hp.max).toBe(10)
        expect(playerUser.activeChar.hp.current).toBe(10)
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        const fetcherPlayer = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetcherPlayer?.activeChar.hp.max).toBe(10)
        expect(fetcherPlayer?.activeChar.hp.current).toBe(1)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(""
          + `Damage before mitigation: 24`
          + `\nMitigated: ${24 - damageTaken}`
          + `\nDamage taken: ${damageTaken}`
          + "\n"
          + `\n**Horu**`
          + `\nHP:   **1/10**`
        )
      })
    })
  })
})