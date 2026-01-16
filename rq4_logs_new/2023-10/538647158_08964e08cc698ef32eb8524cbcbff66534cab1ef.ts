import { isly } from "isly"

export type Balance = Partial<Record<Balance.Entry, number>>

export namespace Balance {
	export type Entry = typeof Entry.values[number]
	export namespace Entry {
		export const values = ["actual", "incomingReserved", "outgoingReserved"] as const
		export const type = isly.string(values)
		export const is = type.is
		export const flaw = type.flaw
	}
	export const type = isly.record<Balance>(Entry.type, isly.number())
	export const is = type.is
	export const flaw = type.flaw
}