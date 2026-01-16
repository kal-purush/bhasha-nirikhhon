import { Mongoose } from "mongoose"

import { connect } from "../../connect"
import logger from "../logger"

export const dbConnect: (dbName: string) => Promise<Mongoose> = (dbName: string = "test") => new Promise((resolve, reject) => {
  // connect to mongodb before tests
  let conn: Mongoose
  beforeAll(async done => {
    try {
      logger.debug(`Utils/Mongo/mongoTest: connecting to database "${dbName}"`)
      // connect to db
      conn = await connect({ dbName })
      resolve(conn)
    } catch (err) {
      reject(err)
    }
    // start tests after connection is estabilished
    done()
  })
})