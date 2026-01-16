import { BASE_THIRD_PARTY_INTERFACE_ADDRESS } from '../enum'
export async function GET() {
  const time = new Date()
  const year = time.getFullYear() //年
  const month = ('0' + (time.getMonth() + 1)).slice(-2) //月
  const day = ('0' + time.getDate()).slice(-2) //日
  const mydate = year + '-' + month + '-' + day
  const fetchResponse = await fetch(
    `${BASE_THIRD_PARTY_INTERFACE_ADDRESS.TODAY_POETRY}one.json?client=npm-sdk/1.0`
  )
  const fetchResponse2 = await fetch(
    `${BASE_THIRD_PARTY_INTERFACE_ADDRESS.EVERYDAY_ENGLISH}index.php?callback=cb&c=dailysentence&m=getdetail&title=${mydate}`
  )
  console.log(fetchResponse2)
  return new Response(
    JSON.stringify({
      [`zh-cn`]: await fetchResponse.json(),
      en: await fetchResponse2
    })
  )
}