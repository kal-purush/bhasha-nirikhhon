import { APIConfig } from "components/APIConfigContext"

export interface ShareLink {
  id: number
  createdAt: string
  validFrom: string
  validTo: string
  url: string
}

export const fetchLinks = async function(
  koodiUri: string,
  type: string,
  apiConfig: APIConfig
): Promise<ShareLink[]> {
  console.log("fetching share links for", koodiUri, type, apiConfig)

  // const { apiUrl } = apiConfig
  // const response = await window.fetch(apiUrl("shareLinks"), {
  //   credentials: "include"
  // })
  // if (!response.ok) {
  //   throw new Error(response.statusText)
  // }
  // return await response.json()

  // TODO: mock data, replace with real API call
  return Promise.resolve([
    {
      id: 1,
      createdAt: "2019-05-01",
      validFrom: "2019-06-01",
      validTo: "2019-06-30",
      url: "https://ehoks.dev/share/1"
    },
    {
      id: 2,
      createdAt: "2019-05-02",
      validFrom: "2019-07-01",
      validTo: "2019-07-08",
      url: "https://ehoks.dev/share/2"
    },
    {
      id: 3,
      createdAt: "2019-05-03",
      validFrom: "2019-09-01",
      validTo: "2019-09-30",
      url: "https://ehoks.dev/share/3"
    }
  ])
}