import { pax2pay } from "../index"

describe("Counterbalances", () => {
	it("Counterbalances.Counterbalance.add", () => {
		const Counterbalance1: pax2pay.Counterbalances.Counterbalance = { "fee.other": 123 }
		const Counterbalance2: pax2pay.Counterbalances.Counterbalance = { "fee.other": 123, "incoming.internal": 789 }
		expect(pax2pay.Counterbalances.Counterbalance.add(Counterbalance1, Counterbalance2, "USD")).toEqual({
			"fee.other": 246,
			"incoming.internal": 789,
		})
	})
	it("Counterbalances.add", () => {
		const Counterbalances1: pax2pay.Counterbalances = {
			USD: { "fee.other": 123 },
			GBP: { "settle.uk-tpl-marqeta": 999 },
			SEK: { "settle.uk-tpl-marqeta": 999 },
		}
		const Counterbalances2: pax2pay.Counterbalances = { USD: { "fee.other": 123, "incoming.internal": 789 } }
		expect(pax2pay.Counterbalances.add(Counterbalances1, Counterbalances2)).toEqual({
			USD: {
				"fee.other": 246,
				"incoming.internal": 789,
			},
			GBP: {
				"settle.uk-tpl-marqeta": 999,
			},
			SEK: {
				"settle.uk-tpl-marqeta": 999,
			},
		})
	})
})