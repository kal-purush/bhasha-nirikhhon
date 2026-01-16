import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"
import { useDbConnection } from "@discord-bot/mongo"

import { test, execute } from "./setDefenseScaling"

describe("Command: setDefenseScaling", () => {
  useDbConnection("Command_setDefenseScaling")

  describe(".test", () => {
    it("works", () => {
      expect(`!set Guardscaling`).toMatch(test)
      expect(`!set guard  scaling`).toMatch(test)
      expect(`!set dodge     scaling`).toMatch(test)
      expect(`!settar evasão     scaling`).toMatch(test)
      expect(`!setdodge  scaling`).toMatch(test)

      expect(`!wrong`).not.toMatch(test)
    })
    it(`captures flags`, () => {
      expect(`!set Guardscaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set Guardscaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!set guard  scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set guard  scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!set dodge     scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!set dodge     scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!settar evasão     scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!settar evasão     scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
      expect(`!setdodge  scaling --bonus 6 --might 2`).toMatch(test)
      expect(test.exec(`!setdodge  scaling --bonus 6 --might 2`)?.groups).toMatchObject({ flags: "--bonus 6 --might 2" })
    })
  })
  
  describe(".execute", () => {
    describe("permissions", () => {
      it(`doesn't allow a non-player, non-DM user to use command`, async () => {
        const [ message ] = mockMessage()
        message.content = `!setguardscaling`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`sorry, but only people with the \"Player\" or \"Dm\" role can use this command`)
      })
      
      it(`doesn't allow a non-DM user to use command with --player flag`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setguardscaling --player "OtherPlayer"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`only DM's can use this command with the --player flag`)
      })
    })
    
    describe("dealing with bad args", () => {
      it(`complains if --char doesn't exist`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setguardscaling --char Kuff`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Player ${message.author.username} doesn't have a character that matches "Kuff". Try "!listChars" to see available characters`)
      })

      it(`complains if --player doesn't exist`, async () => {
        const [message] = mockDmMessage()
        message.content = `!setguardscaling --player "Ragan" --char "Kuff"`
        await execute(message, test.exec(message.content)!)
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`player "Ragan" doesn't exist in my database. are you sure you typed their name correctly?`)
      })
    })
    
    describe("happy paths", () => {
      it('complains if no flag present', async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setguardscaling`
        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        
        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Nothing changed, as you didn't specify any argument for scaling to change.`)
      })

      it("chages dodge scaling", async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setdodgeScaling --will 4.5`

        const playerUser = PlayerUserModel.createUser({ userId: message.author.id, username: message.author.username })
        playerUser.addCharacter(PcModel.createCharacter({ name: "Horu" }))
        playerUser.activeChar.attributes.Will.value = 2
        playerUser.activeChar.updateMaxResources()

        // console.log(playerUser.activeChar.dodgeScaling)
        expect(playerUser.activeChar.dodgeScaling.Will).toBe(0)
        expect(playerUser.activeChar.dodge.total).toBe(0)

        await playerUser.save()
        await execute(message, test.exec(message.content)!)
        
        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! I made the following changes to Horu's Dodge scaling:`
          + "\n\tWill: 0 -> 4.5"
        )
        
        const fetchedUser = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetchedUser?.activeChar.dodgeScaling.Will).toBe(4.5)
        expect(fetchedUser?.activeChar.dodge.total).toBe(9)
      })

      it(`changes hp scaling`, async () => {
        const [message] = mockPlayerMessage()
        message.content = `!setguardscaling
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
        
        expect(playerUser.activeChar.guardScaling.bonus).toBe(0)
        expect(playerUser.activeChar.guardScaling.Might).toBe(1)
        expect(playerUser.activeChar.guard.total).toBe(0)

        await playerUser.save()
        await execute(message, test.exec(message.content)!)

        expect(message.channel.send).toBeCalledTimes(1)
        expect(message.channel.send).toBeCalledWith(`Ok! I made the following changes to Horu's Guard scaling:`
          + "\n\tbase: 0 -> 10"
          + "\n\tbonus: 0 -> 5"
          + "\n\tlevel: 0 -> 1"
          + "\n\thighestPhysical: 0 -> 3.5"
          + "\n\thighestMental: 0 -> 3.5"
          + "\n\thighestSocial: 0 -> 3.5"
          + "\n\thighestSpecial: 0 -> 3.5"
          + "\n\tAgility: 0 -> 1.5"
          + "\n\tFortitude: 0.75 -> 1.5"
          + "\n\tMight: 1 -> 2.5"
          + "\n\tLearning: 0 -> 1.5"
          + "\n\tLogic: 0 -> 1.5"
          + "\n\tPerception: 0 -> 1.5"
          + "\n\tWill: 0 -> 1.5"
          + "\n\tDeception: 0 -> 1.5"
          + "\n\tPersuasion: 0 -> 1.5"
          + "\n\tPresence: 0 -> 1.5"
          + "\n\tAlteration: 0 -> 1.5"
          + "\n\tCreation: 0 -> 1.5"
          + "\n\tEnergy: 0 -> 1.5"
          + "\n\tEntropy: 0 -> 1.5"
          + "\n\tInfluence: 0 -> 1.5"
          + "\n\tMovement: 0 -> 1.5"
          + "\n\tPrescience: 0 -> 1.5"
          + "\n\tProtection: 0.5 -> 1.5"
        )
        
        const fetchedUser = await PlayerUserModel.getUser(playerUser.userId)
        expect(fetchedUser?.activeChar.guardScaling.bonus).toBe(5)
        expect(fetchedUser?.activeChar.guardScaling.Might).toBe(2.5)
        expect(fetchedUser?.activeChar.guard.total).toBe(16)
      })
    })
  })
})