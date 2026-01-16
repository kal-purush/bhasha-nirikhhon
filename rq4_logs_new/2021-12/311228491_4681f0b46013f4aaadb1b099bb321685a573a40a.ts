function hello() {
    return "hello"
}

describe("2nd", () => {
    test("return hello", () => {
        expect(hello()).toBe("hello")
    })
})