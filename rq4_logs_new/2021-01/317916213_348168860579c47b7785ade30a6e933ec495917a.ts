import { mockMessage } from "@discord-bot/discord-mock"

import parseFlags, { FlagsObject } from "../Utils/parseArgs"

describe("Utils: parseArgs", () => {

  it('rejects if message is missing non-optional flag', () => {
    const [message] = mockMessage()
    const flagsStr = ``
    // parse arguments
    const flagsObject: FlagsObject<{ name: string }> = {
      name: { type: "string" },
    }
    const flags = parseFlags("!commandName", flagsObject, flagsStr , message)

    expect(flags).toBe(null)  // expect rejection
    expect(message.channel.send).toBeCalledWith(`"**!commandName**": --name flag is necessary. Please include it and try again`)
  })

  it('rejects if a flag has the wrong type', () => {
    const [message] = mockMessage()
    const flagsStr = `--name "Jonathan"`
    // parse arguments
    const flagsObject: FlagsObject<{ name: number }> = {
      name: { type: "number" },
    }
    const flags = parseFlags("!commandName", flagsObject, flagsStr , message)
    
    expect(flags).toBe(null)  // expect rejection
    expect(message.channel.send).toBeCalledWith(`"**!commandName**": wrong type for flag --name. Flag expects type number, but received type string`)
  })

  it('works by default', () => {
    const [message] = mockMessage()
    const flagsStr = `--name "Jonathan" --age 7`
    // parse arguments
    const flagsObject: FlagsObject<{ name: string, age: number }> = {
      name: { type: "string" },
      age: { type: "number", optional: true },
    }
    const flags = parseFlags("!commandName", flagsObject, flagsStr , message)
    
    expect(flags).toEqual({ name: "Jonathan", age: 7 })
    expect(message.channel.send).not.toBeCalled()
  })

  it('works when excluding optional flags', () => {
    const [message] = mockMessage()
    const flagsStr = `--name "Jonathan" `
    // parse arguments
    const flagsObject: FlagsObject<{ name: string, age: number }> = {
      name: { type: "string" },
      age: { type: "number", optional: true },
    }
    const flags = parseFlags("!commandName", flagsObject, flagsStr , message)
    
    expect(flags).toEqual({ name: "Jonathan", age: undefined })
    expect(flags).toHaveProperty("age", undefined)
    expect(message.channel.send).not.toBeCalled()
  })

  it('works with flags in wrong CaSe', () => {
    const [message] = mockMessage()
    const flagsStr = `--Name "Jonathan" `
    // parse arguments
    const flagsObject: FlagsObject<{ name: string, age: number }> = {
      name: { type: "string" },
      age: { type: "number", optional: true },
    }
    const flags = parseFlags("!commandName", flagsObject, flagsStr , message)
    
    expect(flags).toEqual({ name: "Jonathan", age: undefined })
    expect(flags).toHaveProperty("age", undefined)
    expect(message.channel.send).not.toBeCalled()
  })
})