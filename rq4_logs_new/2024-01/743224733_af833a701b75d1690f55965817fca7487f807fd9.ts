export default function (request: any, response: any) {
  response.status(200).json({
    keepAlive: true
  })
}