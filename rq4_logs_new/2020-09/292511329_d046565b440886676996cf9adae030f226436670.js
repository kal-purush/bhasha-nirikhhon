const debug = require('debug')('convert')
const express = require('express')
const multer = require('multer')
const sharp = require('sharp')

var upload = multer({ storage: multer.memoryStorage() })

const app = express()

app.get('/', (req, res) => {
    res.send('convert API')
})

app.get('/healthcheck', (req, res) => {
    res.json({ uptime: process.uptime() })
})

const contentType = (buffer) => {
    if (!buffer || buffer.length < 12) return
    if (buffer[0] === 255 &&
		buffer[1] === 216 &&
        buffer[2] === 255)
    {
        return 'image/jpeg'
    }
    if (buffer[0] === 0x89 &&
		buffer[1] === 0x50 &&
		buffer[2] === 0x4E &&
		buffer[3] === 0x47 &&
		buffer[4] === 0x0D &&
		buffer[5] === 0x0A &&
		buffer[6] === 0x1A &&
        buffer[7] === 0x0A)
    {
        return 'image/png'
    }
    if (buffer[8] === 87 &&
        buffer[9] === 69 &&
        buffer[10] === 66 &&
        buffer[11] === 80)
    {
        return 'image/webp'
    }
}

// curl --form "file=@test/duck.jpg" --silent http://localhost:4000/greyscale --output duck.jpg
// curl --form "file=@test/duck.png" --silent http://localhost:4000/greyscale --output duck.png
// curl --form "file=@test/duck.webp" --silent http://localhost:4000/greyscale --output duck.webp
app.post('/greyscale', upload.any(), (req, res) => {
    // debug('req.params:', req.params)
    // debug('req.body:', req.body)
    // debug('req.data:', req.data)
    // debug('req.name:', req.name)
    debug('req.files:', req.files)
    if (req.files == undefined || req.files.length == 0) {
        return res.status(400).send('Field file is required')
    }
    const file = req.files.find(e => e.fieldname == 'file')
    debug('file:', file)
    
    const type = contentType(file.buffer)
    debug('type:', type)
    
    if (type === undefined) {
        return res.status(400).send('Invalid format')
    }
    
    sharp(file.buffer)
        .greyscale()
        .toFormat(type.split('/')[1])
        .toBuffer(function (err, buffer) {
            // add a header to display the image rather than download it
            res.header('Content-Type', type)
            return res.send(buffer)
        })
})

module.exports = app