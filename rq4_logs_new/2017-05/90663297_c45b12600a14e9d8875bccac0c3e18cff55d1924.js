
var express = require('express')
var cors = require('cors')
var multer  = require('multer')

var storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, './uploads/')
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname)
  }
})
var upload = multer({ 
	storage : storage,
	dest: 'uploads/',
})
var app = express()
app.use(express.static('uploads'));
app.use(cors())

app.post('/', upload.single('file'), function (req, res) {
	return res.send('OK')
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
});