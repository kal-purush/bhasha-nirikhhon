const axios = require('axios')
const express = require('express')

const app = express()
const cors = require('cors')
const elegant_uri = process.env.ELEGANT_CMS_URL
const token = process.env.ELEGANT_CMS_TOKEN

app.use(cors())

app.get('/events', (req, res) => {
    axios.get(elegant_uri, {       
        headers: {'Authorization': 'Token token=' + token}
    })    
    .then (response => {
        res.json(response.data.data.map(event => event.attributes.fields))
    }) 
    .catch (error => console.log)
})

app.listen(process.env.PORT)