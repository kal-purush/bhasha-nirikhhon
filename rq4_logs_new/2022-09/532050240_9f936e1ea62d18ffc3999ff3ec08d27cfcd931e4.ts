const BASE_URL = 'http://79.143.31.216';

export const register = (username: string, password: string) => {
  return fetch(`${BASE_URL}/register?username=${username}&password=${password}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    }
  })
  .then(checkResponse)
}

export const login = (username: string, password: string) => {
  return fetch(`${BASE_URL}/login`, {
    body: `grant_type=&username=${username}&password=${password}&scope=&client_id=&client_secret=`,
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/x-www-form-urlencoded"
    },
    method: "POST"
  })
  .then(checkResponse)
}

export const squeeze = (link: string, token: string) => {
  return fetch(`${BASE_URL}/squeeze?link=${link}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "Authorization": `bearer ${token}`
    }
  })
  .then(checkResponse)
}

export function checkResponse(response: { ok: any; json: () => any; status: any; }) {
  return response.ok ? response.json() : Promise.reject(`Error ${response.status}`)
}

