import fs from "fs"

export default defineEventHandler(
  async() => {
    throw createError({
      statusCode: 404,
      statusMessage: "Not Found",
    })
  }
)