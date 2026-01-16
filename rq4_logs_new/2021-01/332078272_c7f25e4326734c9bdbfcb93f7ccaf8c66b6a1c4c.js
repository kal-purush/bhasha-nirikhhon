const Jimp = require("jimp")
const formatDistance = require("date-fns/formatDistance")
const capitalize = str => str.charAt(0).toUpperCase() + str.slice(1);

exports.handler = async event => {
  if (event.queryStringParameters.time === undefined) {
    return {
      statusCode: 400,
      body: "(・・;)ゞ",
    }
  }

  const mime = "image/png"
  const font = await Jimp.loadFont(Jimp.FONT_SANS_14_BLACK)
  const text = capitalize(formatDistance(new Date(), new Date(+event.queryStringParameters.time).getTime())) + " ago"

  const width = Jimp.measureText(font, text)
  const height = Jimp.measureTextHeight(font, text)
  const image = await Jimp.read(width, height + 3)

  image.color([{ apply: "lighten", params: [45] }])
  image.filterType(Jimp.PNG_FILTER_NONE)
  image.print(font, 0, 3, text)

  return {
    isBase64Encoded: true,
    statusCode: 200,
    headers: {
      "Content-Type": mime,
    },
    body: (await image.getBase64Async(mime)).split(",", 2).pop(),
  }
}