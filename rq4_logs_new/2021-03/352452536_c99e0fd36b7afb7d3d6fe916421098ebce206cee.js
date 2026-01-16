import app from './src/routes'
require('dotenv').config()

const port = process.env.PORT || 3000

app.listen(port, () => {
  console.log(`My app listening at ${port}`)
})