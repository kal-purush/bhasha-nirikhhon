import { APIConfig } from "components/APIConfigContext"

interface BackendShareLink {
  uuid: string
  "voimassaolo-alku": string
  "voimassaolo-loppu": string
  tyyppi: string
}

export interface ShareLink {
  uuid: string
  validFrom: string
  validTo: string
  type: string
}

export const fetchLinks = async function(
  eid: string,
  koodiUri: string,
  type: string,
  apiConfig: APIConfig
): Promise<ShareLink[]> {
  const { apiUrl, apiPrefix } = apiConfig
  const response = await window.fetch(
    apiUrl(`${apiPrefix}/hoksit/${eid}/share/${koodiUri}`),
    {
      credentials: "include"
    }
  )
  if (!response.ok) {
    throw new Error(response.statusText)
  }
  const json: { data: BackendShareLink[] } = await response.json()
  return json.data
    .filter(link => {
      return link.tyyppi === type
    })
    .map(link => {
      return {
        uuid: link.uuid,
        validFrom: link["voimassaolo-alku"],
        validTo: link["voimassaolo-loppu"],
        type: link.tyyppi
      }
    })
}

export const createLink = async function({
  eid,
  koodiUri,
  startDate,
  endDate,
  type,
  apiConfig
}: {
  eid: string
  koodiUri: string
  startDate: string
  endDate: string
  type: string
  apiConfig: APIConfig
}): Promise<string> {
  const { apiUrl, apiPrefix } = apiConfig
  const response = await window.fetch(
    apiUrl(`${apiPrefix}/hoksit/${eid}/share/${koodiUri}`),
    {
      credentials: "include",
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        "voimassaolo-alku": startDate,
        "voimassaolo-loppu": endDate,
        tyyppi: type
      })
    }
  )

  if (!response.ok) {
    throw new Error(response.statusText)
  }
  const json = await response.json()
  return json.meta.uuid
}

export const removeLink = async function({
  eid,
  koodiUri,
  uuid,
  apiConfig
}: {
  eid: string
  koodiUri: string
  uuid: String
  apiConfig: APIConfig
}) {
  const { apiUrl, apiPrefix } = apiConfig
  const response = await window.fetch(
    apiUrl(`${apiPrefix}/hoksit/${eid}/share/${koodiUri}/${uuid}`),
    {
      credentials: "include",
      method: "DELETE",
      headers: {
        "Content-Type": "application/json"
      }
    }
  )

  if (!response.ok) {
    throw new Error(response.statusText)
  }
}