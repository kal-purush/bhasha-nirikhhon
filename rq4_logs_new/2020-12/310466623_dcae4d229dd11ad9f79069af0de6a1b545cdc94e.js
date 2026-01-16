const multer = require('multer')

const upload = multer({
  dest: 'avatar',
})

module.exports = upload