import mongoose from 'mongoose'

interface IMongoOptions {
  useNewUrlParser?: boolean
  useUnifiedTopology?: boolean
}

const defaultOptions: IMongoOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true
}

function connect(connectionString: string, options: IMongoOptions = defaultOptions): Promise<boolean> {
  return new Promise((resolve, reject) => {
    mongoose.connect(connectionString, options).then(() => {
      console.log(`Mongo on ${connectionString} connected`)
      resolve(true)
    }, (err) => {
      console.error('Error connecting to MongoDB', err)
      reject(false)
    })

  })
}

function disconnect():Promise<void> {
  return mongoose.disconnect()
}

export { connect, disconnect, IMongoOptions }