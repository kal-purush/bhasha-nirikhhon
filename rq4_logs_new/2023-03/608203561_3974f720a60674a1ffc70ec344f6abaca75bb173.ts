import { BASE_THIRD_PARTY_INTERFACE_ADDRESS } from '../enum'
const querystring = require('querystring')
export async function GET(request: Request) {
  const queryValue = querystring.parse(request.url.split('?')[1])
  const { longitude, latitude } = queryValue
  const fetchResponse = await fetch(
    `${BASE_THIRD_PARTY_INTERFACE_ADDRESS.COLORFUL_CLOUD_WEATHER}v2.6/TAkhjf8d1nlSlspN/${longitude},${latitude}/daily?dailysteps=4`
  )
  return new Response(JSON.stringify(await fetchResponse.json()))
}