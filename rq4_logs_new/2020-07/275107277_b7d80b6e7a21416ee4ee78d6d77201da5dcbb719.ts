import 'jest'
import { readFileSync } from 'fs'
import { Pool, Client } from 'pg'
import { getPgClient, getPgPool } from '../src/helpers/connect-pg'

describe("About Database Connections", () => {
    it("At least one valid db info", () => {
        const file = JSON.parse(readFileSync('db.properties', { encoding: "utf8" }))
        const connections = Object.keys(file)
        const connection = connections[0]

        expect(file).not.toBe(undefined)
        expect(connections.length).toBeGreaterThan(0)
        expect(connection.hasOwnProperty("drivername")).not.toBe(undefined)
        expect(connection.hasOwnProperty("host")).not.toBe(undefined)
    })
    it("Get Client", async () => {
        const client: Client = getPgClient()
        try {
            await client.connect()
            await client.query('SELECT NOW()')
            await client.end()
        }
        catch (e) {
            expect(client).toThrow(new Error("Cannot connect to DB"))
        }

    })
    it("Get Pool", async () => {
        const pool: Pool = getPgPool()
        try {
            const client = await pool.connect()
            await client.query('SELECT NOW()')
            client.release()
            await pool.end()
        }
        catch (e) {
            expect(pool).toThrow(new Error("Cannot connect to DB"))
        }
    })
})