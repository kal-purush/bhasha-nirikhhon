const path = require('path')
const dotenv = require('dotenv')

const express = require('express')
const multer = require('multer')

dotenv.config()
const pool = require('./db')
const { table, port } = process.env
const PORT = port


const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, './upload/')
    },
    filename: (req, file, cb) => {
        const filename = Date.now() + path.extname(file.originalname)
        req.filename = filename
        cb(null, filename)
    }
})
const upload = multer({
    storage
}).single('file')

const app = express()
app.use('/image', express.static('upload'))

app.post('/upload', (req, res, next) => {
    upload(req, res, function (err) {
        if (err) {
            res.status(416).send({
                errCode: 416,
                errMsg: '文件上传失败'
            })
        } else {
            const imgUrl = `localhost:8081/image/${req.filename}`
            const sql = `insert into ${table} (img_url) values (?)`
            pool.execute(sql, [imgUrl]).then(result => {
                res.status(200).send({
                    filepath: imgUrl,
                    status: 200,
                    success: '文件上传成功'
                })
            })

        }
    })
})


app.get('/queryImg', async (req, res, next) => {
    const { id } = req.query
    console.log(id)
    const sqlId = `select * from ${table} where id=?`
    const sqlAll = `select * from ${table}`
    let result
    if (id) {
        result = await pool.execute(sqlId, [id])
    } else {
        result = await pool.execute(sqlAll)
    }
    res.send({ result: result[0], code: 200, msg: 'success' })
})

app.listen(PORT, () => {
    console.log(`localhost:${PORT}`)
})