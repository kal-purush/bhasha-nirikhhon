import { Action } from "../Action"
import { Formatter } from "../Formatter"
import { get } from "./index"

describe("card-csc", () => {
	const handler = get("card-csc") as Formatter
	it("key event first key", () => {
		const result = Action.apply(handler, { value: "", selection: { start: 0, end: 0 } }, { key: "1" })
		expect(result).toMatchObject({ value: "1", selection: { start: 1, end: 1 } })
	})
	it("key event full csc", () => {
		let result = { value: "", selection: { start: 0, end: 0 } }
		for (const character of "987")
			result = Action.apply(handler, result, { key: character })
		expect(result).toMatchObject({ value: "987", selection: { start: 3, end: 3 } })
	})
	it("key event full csc with extra chars", () => {
		let result = { value: "", selection: { start: 0, end: 0 } }
		for (const character of "987242429999")
			result = Action.apply(handler, result, { key: character })
		expect(result).toMatchObject({ value: "987", selection: { start: 3, end: 3 } })
	})
	it("key event backspace", () => {
		let result = { value: "987", selection: { start: 3, end: 3 } }
		result = Action.apply(handler, result, { key: "Backspace" })
		expect(result).toMatchObject({ value: "98", selection: { start: 2, end: 2 } })
	})
	it("key event backspace in the middle", () => {
		let result = { value: "98", selection: { start: 2, end: 2 } }
		result = Action.apply(handler, result, { key: "Backspace" })
		expect(result).toMatchObject({ value: "9", selection: { start: 1, end: 1 } })
	})
})