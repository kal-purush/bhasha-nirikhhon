import { Action } from "../Action"
import { Formatter } from "../Formatter"
import { get } from "./index"

describe("password", () => {
	const handler = get("password") as Formatter
	it("key event first key", () => {
		const result = Action.apply(handler, { value: "", selection: { start: 0, end: 0 } }, { key: "1" })
		expect(result).toMatchObject({ value: "1", selection: { start: 1, end: 1 } })
	})
	it("key event longer string", () => {
		let result = { value: "", selection: { start: 0, end: 0 } }
		for (const character of "alfa123")
			result = Action.apply(handler, result, { key: character })
		expect(result).toMatchObject({ value: "alfa123", selection: { start: 7, end: 7 } })
	})
	it("key event backspace", () => {
		let result = { value: "alfa123", selection: { start: 7, end: 7 } }
		result = Action.apply(handler, result, { key: "Backspace" })
		expect(result).toMatchObject({ value: "alfa12", selection: { start: 6, end: 6 } })
	})
})