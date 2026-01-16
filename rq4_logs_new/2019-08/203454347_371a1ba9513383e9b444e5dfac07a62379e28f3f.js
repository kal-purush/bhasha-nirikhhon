const router = require('express').Router()
const {models, connectDb} = require('../../src/models')

const path = require('path')
const DIST_DIR = path.join(__dirname, '../../dist')
const HTML_FILE = path.join(DIST_DIR, 'index.html')

connectDb().then(async () => {
  app.listen(process.env.PORT, () =>
    console.log(`Example app listening on port ${process.env.PORT}!`),
  );
});

router.route('/').get((req, res) => {
  res.sendFile(HTML_FILE)
})

router.route('/').post((req, res) => {
  console.log("new user: ", req.body)
  res.send(req.body)
  models.User.create(req.body).then((user) => {
    res.send(user)
  })
})

module.exports = router;