import { mockMessage } from "@discord-bot/discord-mock"
import AsciiTable from "ascii-table"
import { damageDice } from "Models/PlayerCharacter"
import { useDbConnection } from "Utils/Mongo/mongoTest"

import { test, execute, createDamageTable } from "./dmgTable"

describe("Command: dmgTable", () => {
  useDbConnection("Command_dmgTable")

  describe(".test", () => {
    it("works", () => {
      expect(`!wrong`).not.toMatch(test)
      expect(`!dmgtable`).toMatch(test)
      expect(`!dmgtable 5`).toMatch(test)
      expect(test.exec(`!dmgtable 5`)?.groups).toEqual({ number: "5" })
      expect(`!dmgtable    13`).toMatch(test)
      expect(test.exec(`!dmgtable    13`)?.groups).toEqual({ number: "13" })
      expect(`!damageTable    0`).toMatch(test)
      expect(test.exec(`!damageTable    0`)?.groups).toEqual({ number: "0" })
      expect(`!dmgTable 9999`).toMatch(test)
      expect(test.exec(`!dmgTable 9999`)?.groups).toEqual({ number: "9999" })
    })
  })

  describe('.execute', () => {
    it('works for looking at the dmgTable', async () => {
      const [message] = mockMessage()
      message.content = `!dmgTable`
      await execute(message, test.exec(message.content)!)
      const damageTable = createDamageTable()
      expect(message.channel.send).toBeCalledTimes(1)
      expect(message.channel.send).toBeCalledWith(""
        + "```\n"
        + damageTable.toString()
        + "```"
      )
    })
    it('works for default values (0-20)', async () => {
      const [message1] = mockMessage()
      message1.content = `!dmgTable 0`
      await execute(message1, test.exec(message1.content)!)
      expect(message1.channel.send).toBeCalledTimes(1)
      const messageSent1 = (message1.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
      expect(messageSent1).toEqual(expect.stringContaining("__**1d2!**__:"))

      const [message2] = mockMessage()
      message2.content = `!dmgTable 13`
      await execute(message2, test.exec(message2.content)!)
      expect(message2.channel.send).toBeCalledTimes(1)
      const messageSent2 = (message2.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
      expect(messageSent2).toEqual(expect.stringContaining("__**10d4!**__:"))
    })

    it('works for values above the max (21+)', async () => {
      const [message2] = mockMessage()
      message2.content = `!dmgTable 999`
      await execute(message2, test.exec(message2.content)!)
      expect(message2.channel.send).toBeCalledTimes(1)
      const messageSent2 = (message2.channel.send as unknown as jest.MockedFunction<(a:string) => {}>).mock.calls[0][0]
      expect(messageSent2).toEqual(expect.stringContaining("__**10d10!**__:"))
    })
  })
})