const request = require("supertest")
const db = require("../data/dbConfig")
const server = require("./server")

it("correct env", ()=>{
    expect(process.env.DB_ENV).toBe("testing")
})