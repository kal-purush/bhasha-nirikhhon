const express = require('express')
const nunjucks = require('nunjucks')

const app = express()

nunjucks.configure('views', {
  autoescape: true,
  express: app,
  watch: true
})

app.set('view engine', 'njk')
app.use(express.urlencoded({ extended: false }))

const checkAgeMiddleware = (req, res, next) => {
  const { age } = req.body
  if (!age) return res.redirect('/')
  return next()
}

app.get('/', (req, res) => {
  return res.render('index')
})

app.post('/check', checkAgeMiddleware, (req, res) => {
  const { age } = req.body
  if (age >= 18) return res.redirect(`/major/?age=${age}`)

  return res.redirect(`/minor/?age=${age}`)
})

app.get('/major', (req, res) => {
  const { age } = req.query
  return res.render('major', { age })
})

app.get('/minor', (req, res) => {
  const { age } = req.query
  return res.render('minor', { age })
})

app.listen(3000)