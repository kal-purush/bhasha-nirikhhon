import { isoly } from "isoly"
import { isly } from "isly"

export type Indices =
	| ["currency", isoly.Currency]
	| ["direction", Indices.Direction]
	| ["rail", Indices.Rail]
	| ["status", Indices.Status]
export namespace Indices {
	const direction = ["inbound", "outbound"] as const
	export type Direction = typeof direction[number]
	const rail = "internal"
	export type Rail = typeof rail
	const status = ["review", "created", "approved", "rejected", "processing", "finalized"] as const
	export type Status = typeof status[number]
	export const type = isly.union(
		isly.tuple<Indices>(isly.string("currency"), isly.fromIs("isoly.Currency", isoly.Currency.is)),
		isly.tuple<Indices>(isly.string("direction"), isly.string(direction)),
		isly.tuple<Indices>(isly.string("rail"), isly.string(rail)),
		isly.tuple<Indices>(isly.string("status"), isly.string(status))
	)
	export const is = type.is
	export const flaw = type.flaw
	export function parse(value: string | undefined): Indices | undefined {
		const indices = value?.split(",")
		return is(indices) ? indices : undefined
	}
}