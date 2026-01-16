/*

ts-node -P scripts ./scripts/cloudinary.ts

curl 'https://api.cloudinary.com/v1_1/kirill/resources/image'

 */

require('dotenv').config()
import * as P from 'bluebird'
import * as fs from 'fs-extra'
import { rootDir } from '../src/rootDir'
import * as cloudinary from 'cloudinary'
import * as util from 'util'
import * as path from 'path'
import * as klawSync from 'klaw-sync'
const imgDir = rootDir + '/src/static/img'

const started = Date.now()

cloudinary.config({
  cloud_name: 'kirill',
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
})

const upload = util.promisify(cloudinary.v2.uploader.upload)
const list = util.promisify(cloudinary.v2.api.resources)

const concurrency = 1

list({
  max_results: 500,
})
  .then(r => {
    const images = r.resources.map(_r => _r.secure_url)
    console.log(images)
  })

async function doUpload () {
  let files = klawSync(imgDir, {
    nodir: true,
  })
    .map(i => i.path)
    .filter(f => !path.parse(f).name.startsWith('.'))

  await P
    .map(files, async f => {
      const p = path.parse(f)
      // console.log(f, path.parse(f))
      const dir = p.dir.substring(imgDir.length + 1)
      const public_id = `${dir}/${p.name}`
      // console.log(public_id)

      const r = await upload(f, {
        public_id,
        unique_filename: false,
      })
      console.log(`<< ${public_id}`)
    })
    .then(r => {
      // console.log(r)
      const sec = Math.round((Date.now() - started) / 1000)
      console.log(`done in ${sec} sec using ${concurrency} concurrency`)
    })
}