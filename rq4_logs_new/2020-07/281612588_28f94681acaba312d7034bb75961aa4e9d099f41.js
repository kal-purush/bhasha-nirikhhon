const { Storage } = require("@google-cloud/storage")

module.exports = (publicationId, storage = new Storage()) => {
  const bucket = storage.bucket(publicationId)

  return bucket.create()
}