import routes from './routes/routes'
import express from 'express'
import mongoose from 'mongoose'
import cors from 'cors'
import dotenv from 'dotenv'


dotenv.config()
const PORT = 7000
const server = express()
const mongoString: string = process.env.URL || ""
const corsOptons  = {
    origin: 'http://localhost:3000',
    optionsSuccessStatus: 200
}

// conexão com o banco de dados
mongoose.connect(mongoString)
const database = mongoose.connection

// iniciando um possivel erro no banco de dados
database.on('err', (err) => {
    console.log(err);
})

// se o banco estiver conectado, vamos receber uma mensagem no log
database.once('connected', () => [
    console.log('DB conected')
])

// usar cors e deixar o front-end usar
server.use(cors(corsOptons))

// para fazer o express entender JSON 
server.use(express.json())

// para usar as rotas feitas em routes/routes.ts
server.use("/api", routes)


// rodamos o servidor node aqui
server.listen(PORT, () => {
    console.log(`server is running in port: ${PORT}`)  
})