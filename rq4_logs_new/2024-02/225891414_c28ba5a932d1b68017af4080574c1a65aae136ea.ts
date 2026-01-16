import { cryptly } from "./index"

describe("Base32", () => {
	it("encode standard 1", () =>
		expect(cryptly.Base32.encode("This is the data (*)")).toEqual("KRUGS4ZANFZSA5DIMUQGIYLUMEQCQKRJ"))
	it("encode standard 2", () =>
		expect(cryptly.Base32.encode("any carnal pleasure.")).toEqual("MFXHSIDDMFZG4YLMEBYGYZLBON2XEZJO"))
	it("encode standard 3", () =>
		expect(cryptly.Base32.encode("any carnal pleasure")).toEqual("MFXHSIDDMFZG4YLMEBYGYZLBON2XEZI"))
	it("encode standard 4", () =>
		expect(cryptly.Base32.encode("any carnal pleasur")).toEqual("MFXHSIDDMFZG4YLMEBYGYZLBON2XE"))
	it("decode standard 1", () =>
		expect(new cryptly.TextDecoder().decode(cryptly.Base32.decode("KRUGS4ZANFZSA5DIMUQGIYLUMEQCQKRJ"))).toEqual(
			"This is the data (*)"
		))
	it("decode standard 2", () =>
		expect(new cryptly.TextDecoder().decode(cryptly.Base32.decode("MFXHSIDDMFZG4YLMEBYGYZLBON2XEZJO"))).toEqual(
			"any carnal pleasure."
		))
	it("decode standard 3", () =>
		expect(new cryptly.TextDecoder().decode(cryptly.Base32.decode("MFXHSIDDMFZG4YLMEBYGYZLBON2XEZI"))).toEqual(
			"any carnal pleasure"
		))
	it("decode standard 4", () =>
		expect(new cryptly.TextDecoder().decode(cryptly.Base32.decode("MFXHSIDDMFZG4YLMEBYGYZLBON2XE"))).toEqual(
			"any carnal pleasur"
		))
	it("xor", () =>
		expect(
			cryptly.Base32.xor(["5468697320697320746865206461746120282a29", "616e79206361726e616c20706c656173757265"])
		).toMatchInlineSnapshot(`"577QAAAEAAAASEAACQAABCACBAAAADRAAACAFACQ"`))
	it("bytewiseAdd", () =>
		expect(
			cryptly.Base32.bytewiseAdd(["5468697320697320746865206461746120282a29", "616e79206361726e616c20706c656173757265"])
		).toMatchInlineSnapshot(`"577757X2737PL3X65L7P45X4637P54G6735PY7VO"`))
	it("add", () =>
		expect(
			cryptly.Base32.add(["5468697320697320746865206461746120282a29", "616e79206361726e616c20706c656173757265"])
		).toMatchInlineSnapshot(`"577777737777N3775P776575677774O77757275O"`))
	it("combine", () =>
		expect(
			cryptly.Base32.combine(["5468697320697320746865206461746120282a29", "616e79206361726e616c20706c656173757265"])
		).toMatchInlineSnapshot(`"5777777T7777NT775P774Z7547777WO77757S75M"`))
})