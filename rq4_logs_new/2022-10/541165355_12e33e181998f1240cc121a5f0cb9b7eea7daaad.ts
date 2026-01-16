import { BlobServiceClient } from '@azure/storage-blob'
import multiparty from 'multiparty'
import fs from 'fs'

export default async function _(req, res) {
  const form = new multiparty.Form()

  const data = await new Promise((resolve, reject) => {
    form.parse(req, function (err, fields, files) {
      if (err) reject({ err })
      resolve({ fields, files })
    })
  })

  console.debug(`Form data fields: `, data.fields)
  console.debug(`Form data files : `, data.files.image)

  const image = data.files.image[0]
  const imageBinaryData = fs.readFileSync(image.path)

  const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING)
  const containerClient = blobServiceClient.getContainerClient('images')
  const blockBlobClient = containerClient.getBlockBlobClient('ingredients/' + image.originalFilename)
  const uploadBlobResponse = await blockBlobClient.upload(imageBinaryData, image.size)

  console.debug(uploadBlobResponse)

  // List the blob(s) in the container.
  for await (const blob of containerClient.listBlobsFlat()) {
    // Get Blob Client from name, to get the URL
    const tempBlockBlobClient = containerClient.getBlockBlobClient(blob.name)

    // Display blob name and URL
    console.debug(`\n\tname: ${blob.name}\n\tURL: ${tempBlockBlobClient.url}\n`)
  }

  return res.status(200).json({ data })
}

export const config = {
  api: {
    bodyParser: false,
  },
}