import { FastifyInstance } from 'fastify'
import { CallError } from '../helpers/callError'
import { randomUUID } from 'crypto'
import { resolve } from 'node:path'
import { createWriteStream } from 'node:fs'
import { promisify } from 'node:util'
import { pipeline } from 'node:stream'

const pump = promisify(pipeline)
export async function Upload(app: FastifyInstance) {
  app.post('/image', async (request, reply) => {
    const upload = await request.file({
      limits: {
        fileSize: 5_242_800, // 5mb
      },
    })
    console.log(upload)
    if (!upload) {
      return reply.status(400).send()
    }

    const mimeTypeRegex = /^(image|video)\/[a-zA-Z]+/
    const isValidFileFormat = mimeTypeRegex.test(upload.mimetype)

    if (!isValidFileFormat) {
      throw new CallError('Invalid file type.', 400)
    }
    const fileId = new Date().getTime().toString()

    const fileName = fileId.concat(upload.filename)

    console.log(fileName)

    const writeStream = createWriteStream(
      resolve(__dirname, '..', '..', 'uploads', fileName),
    )
    await pump(upload.file, writeStream)

    const fullUrl = request.protocol.concat('://').concat(request.hostname)
    const fileUrl = new URL(`/uploads/${fileName}`, fullUrl).toString()

    return { fileUrl }
  })
}