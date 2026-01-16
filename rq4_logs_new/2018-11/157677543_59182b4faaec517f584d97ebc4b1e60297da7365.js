const express = require('express')
const port = 8000
const app = express()


app.use(express.static('public'))

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/public/index.html')
})

app.use(
    '/public', 
    express.static(__dirname + '/public')
)

app.listen(port, () => {
    console.log(`Listening on ${port}`)
})