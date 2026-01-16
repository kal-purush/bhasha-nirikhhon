const express = require('express')
const hbs = require('hbs')
const fs = require('fs')

var app = express()

hbs. registerPartials(__dirname + '/views/partials')
app.set('view engine', 'hbs')
app.use(express.static(__dirname + '/public'))

app.use((req, res, next) => {
    var now = new Date().toString()
    var log = `${now}: ${req.method} ${req.url}`
    console.log(log)
    fs.appendFileSync('server.log', log + '\n')
    next()
})

// app.use((req, res, next) => {
//     res.render('maintenence.hbs')
// })

hbs.registerHelper('getCurrentYear', () => {
    return new Date().getFullYear()
})

hbs.registerHelper('screamIt', (text) => {
    return text.toUpperCase()
})

app.get('/', (req, res) => {
    // res.send('<h1>Hello frodo!</h1>')
    res.render('home.hbs', {
        name: 'andrew',
        pageTitle: 'Home Page',
        welcomeMessage: 'This is my home page.'
    })
})

app.get('/about', (req, res) => {
    res.render('about.hbs', {
        pageTitle: 'About Page'
    })
})

app.listen(3000)