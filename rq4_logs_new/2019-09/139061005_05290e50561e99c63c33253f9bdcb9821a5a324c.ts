import { koodistoUrls } from "./formConfig"
import { buildKoodiUris, mapKoodiUri } from "./helpers/helpers"

export async function fetchKoodiUris() {
  const requests = await Promise.all(
    Object.keys(koodistoUrls).map(async (key: keyof typeof koodistoUrls) => {
      const json = await window.fetch(koodistoUrls[key]).then(r => r.json())
      return {
        key,
        value: json.data
          .filter((k: { tila: string }) => k.tila !== "PASSIIVINEN")
          .map(mapKoodiUri)
      }
    })
  )
  return requests.reduce<{ [key in keyof typeof koodistoUrls]: any[] }>(
    (koodiUris, koodiUriObj) => {
      koodiUris[koodiUriObj.key] = koodiUriObj.value
      return koodiUris
    },
    buildKoodiUris()
  )
}