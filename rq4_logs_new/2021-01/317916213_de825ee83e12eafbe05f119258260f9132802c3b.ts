import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/mockMessage"
import { useDbConnection } from "Utils/mongoTest"

import { test, execute } from "./setResourceScaling"

describe("Command: setResourceScaling", () => {
  useDbConnection("Command_setResourceScaling")

  describe(".test", () => {
    it("works", () => {
      expect(`!set Hpscaling`).toMatch(test)
      expect(`!set Vida  scaling`).toMatch(test)
      expect(`!set kI     scaling`).toMatch(test)
      expect(`!settar chackra     scaling`).toMatch(test)
      expect(`!setmana  scaling`).toMatch(test)

      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!set Hpscaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set Hpscaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!set Vida  scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set Vida  scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!set kI     scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set kI     scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!settar chackra     scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!settar chackra     scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!setmana  scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!setmana  scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
    })
  })
  
  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!sethpscaling`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!sethpscaling --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!sethpscaling --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!sethpscaling --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it('complains if no flag present', async () => {
        const [message] = mockPlayerMessage()
        message.content = `!sethpscaling`
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Nothing changed, as you didn't specify any argument for scaling to change.`)
      })

      it("chages mp scaling", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setMpScaling --will 4.5`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.activeChar.attributes.Will.value = 2
        playerUser.activeChar.updateMaxResources()

        expect(playerUser.activeChar.mpScaling.Will).toBe(1)
        expect(playerUser.activeChar.mp.max).toBe(12)

        await playerUser.save()
        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! I made the following changes to Horu's MP scaling:`
          + "\n\tWill: 1 -> 4.5"
        )
        
        const fetchedUser = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetchedUser?.activeChar.mpScaling.Will).toBe(4.5)
        expect(fetchedUser?.activeChar.mp.max).toBe(19)
      })

      it(`changes hp scaling`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!sethpscaling
          --bonus 5
          --base 10
          --level 1

          --highest-physical 3.5
          --highest-mental 3.5
          --highest-social 3.5
          --highest-special 3.5

          --agility    1.5
          --fortitude  1.5
          --Might      2.5
          --Learning   1.5
          --Logic      1.5
          --perception 1.5
          --Will       1.5
          --Deception  1.5
          --Persuasion 1.5
          --Presence   1.5
          --alteration 1.5
          --Creation   1.5
          --Energy     1.5
          --entropy    1.5
          --Influence  1.5
          --Movement   1.5
          --prescience 1.5
          --Protection 1.5
        `.replace(/\n/g, "")

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        
        expect(playerUser.activeChar.hpScaling.bonus).toBe(0)
        expect(playerUser.activeChar.hpScaling.Might).toBe(1.5)
        expect(playerUser.activeChar.hp.max).toBe(10)

        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! I made the following changes to Horu's HP scaling:`
          + "\n\tbase: 8 -> 10"
          + "\n\tbonus: 0 -> 5"
          + "\n\tlevel: 2 -> 1"
          + "\n\thighestPhysical: 0 -> 3.5"
          + "\n\thighestMental: 0 -> 3.5"
          + "\n\thighestSocial: 0 -> 3.5"
          + "\n\thighestSpecial: 0 -> 3.5"
          + "\n\tAgility: 0 -> 1.5"
          + "\n\tFortitude: 2 -> 1.5"
          + "\n\tMight: 1.5 -> 2.5"
          + "\n\tLearning: 0 -> 1.5"
          + "\n\tLogic: 0 -> 1.5"
          + "\n\tPerception: 0 -> 1.5"
          + "\n\tWill: 1 -> 1.5"
          + "\n\tDeception: 0 -> 1.5"
          + "\n\tPersuasion: 0 -> 1.5"
          + "\n\tPresence: 1.5 -> 1.5"
          + "\n\tAlteration: 0 -> 1.5"
          + "\n\tCreation: 0 -> 1.5"
          + "\n\tEnergy: 0 -> 1.5"
          + "\n\tEntropy: 0 -> 1.5"
          + "\n\tInfluence: 0 -> 1.5"
          + "\n\tMovement: 0 -> 1.5"
          + "\n\tPrescience: 0 -> 1.5"
          + "\n\tProtection: 0 -> 1.5"
        )
        
        const fetchedUser = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetchedUser?.activeChar.hpScaling.bonus).toBe(5)
        expect(fetchedUser?.activeChar.hpScaling.Might).toBe(2.5)
        expect(fetchedUser?.activeChar.hp.max).toBe(16)
      })
    })
  })
})