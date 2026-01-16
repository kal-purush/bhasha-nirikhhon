import { Value, ValueType } from "./Value"

describe("Value", () => {
  describe("Value.bool", () => {
    it("contains a falsy boolean value", () => {
      const value = Value.bool(false)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0n)

      expect(value.bool).toBe(false)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(value.uint8).toBe(0)
      expect(value.uint16).toBe(0)
      expect(value.uint32).toBe(0)
      expect(value.uint64).toBe(0n)
    })

    it("contains a truthy boolean value", () => {
      const value = Value.bool(true)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(1n)

      expect(value.bool).toBe(true)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(value.uint8).toBe(1)
      expect(value.uint16).toBe(1)
      expect(value.uint32).toBe(1)
      expect(value.uint64).toBe(1n)
    })
  })

  describe("Value.byte", () => {
    it("contains a bytearray", () => {
      const bytearray = Uint8Array.from([97, 98, 99])
      const value = Value.byte(bytearray)
      expect(value.type).toBe(ValueType.BYTE)
      expect(value.value).toBe(bytearray)

      expect(() => value.bool).toThrow(/invalid type/i)
      expect(value.byte).toBe(bytearray)
      expect(() => value.uint8).toThrow(/invalid type/i)
      expect(() => value.uint16).toThrow(/invalid type/i)
      expect(() => value.uint32).toThrow(/invalid type/i)
      expect(() => value.uint64).toThrow(/invalid type/i)
    })

    it("throws if bytearray exceeds the maximum length", () => {
      const bytearray = Uint8Array.from(Array(4097).fill(0))
      expect(() => Value.byte(bytearray)).toThrow(/length exceeded/i)
    })
  })

  describe("Value.uint8", () => {
    it("contains a 8-byte integer (non-zero)", () => {
      const value = Value.uint8(0xff)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0xffn)

      expect(value.bool).toBe(true)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(value.uint8).toBe(0xff)
      expect(value.uint16).toBe(0xff)
      expect(value.uint32).toBe(0xff)
      expect(value.uint64).toBe(0xffn)
    })

    it("throws if value is NaN", () => {
      expect(() => Value.uint8(NaN)).toThrow(/not an integer/i)
    })

    it("throws if value is not an integer", () => {
      expect(() => Value.uint8(3.14)).toThrow(/not an integer/i)
    })

    it("throws if value is negative", () => {
      expect(() => Value.uint8(-5)).toThrow(/overflow/i)
    })

    it("throws if value is too large", () => {
      expect(() => Value.uint8(0x100)).toThrow(/overflow/i)
    })
  })

  describe("Value.uint16", () => {
    it("contains a 16-byte integer (non-zero)", () => {
      const value = Value.uint16(0xffff)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0xffffn)

      expect(value.bool).toBe(true)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(() => value.uint8).toThrow(/overflow/i)
      expect(value.uint16).toBe(0xffff)
      expect(value.uint32).toBe(0xffff)
      expect(value.uint64).toBe(0xffffn)
    })

    it("throws if value is NaN", () => {
      expect(() => Value.uint16(NaN)).toThrow(/not an integer/i)
    })

    it("throws if value is not an integer", () => {
      expect(() => Value.uint16(3.14)).toThrow(/not an integer/i)
    })

    it("throws if value is negative", () => {
      expect(() => Value.uint16(-5)).toThrow(/overflow/i)
    })

    it("throws if value is too large", () => {
      expect(() => Value.uint16(0x10000)).toThrow(/overflow/i)
    })
  })

  describe("Value.uint32", () => {
    it("contains a 32-byte integer (non-zero)", () => {
      const value = Value.uint32(0xffffffff)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0xffffffffn)

      expect(value.bool).toBe(true)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(() => value.uint8).toThrow(/overflow/i)
      expect(() => value.uint16).toThrow(/overflow/i)
      expect(value.uint32).toBe(0xffffffff)
      expect(value.uint64).toBe(0xffffffffn)
    })

    it("throws if value is NaN", () => {
      expect(() => Value.uint32(NaN)).toThrow(/not an integer/i)
    })

    it("throws if value is not an integer", () => {
      expect(() => Value.uint32(3.14)).toThrow(/not an integer/i)
    })

    it("throws if value is negative", () => {
      expect(() => Value.uint32(-5)).toThrow(/overflow/i)
    })

    it("throws if value is too large", () => {
      expect(() => Value.uint32(0x100000000)).toThrow(/overflow/i)
    })
  })

  describe("Value.uint64", () => {
    it("contains a 64-byte integer (non-zero)", () => {
      const value = Value.uint64(0xffffffffffffffffn)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0xffffffffffffffffn)

      expect(value.bool).toBe(true)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(() => value.uint8).toThrow(/overflow/i)
      expect(() => value.uint16).toThrow(/overflow/i)
      expect(() => value.uint32).toThrow(/overflow/i)
      expect(value.uint64).toBe(0xffffffffffffffffn)
    })

    it("contains a 64-byte integer (zero)", () => {
      const value = Value.uint64(0n)
      expect(value.type).toBe(ValueType.UINT)
      expect(value.value).toBe(0n)

      expect(value.bool).toBe(false)
      expect(() => value.byte).toThrow(/invalid type/i)
      expect(value.uint8).toBe(0)
      expect(value.uint16).toBe(0)
      expect(value.uint32).toBe(0)
      expect(value.uint64).toBe(0n)
    })

    it("throws if value is negative", () => {
      expect(() => Value.uint64(-5n)).toThrow(/overflow/i)
    })

    it("throws if value is too large", () => {
      expect(() => Value.uint64(0x10000000000000000n)).toThrow(/overflow/i)
    })
  })
})