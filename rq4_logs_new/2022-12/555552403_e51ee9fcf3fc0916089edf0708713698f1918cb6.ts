export const baseUrl = 'https://dogsapi.origamid.dev/json'



type TokenPostProps = {
username: string;
password:string;
}

export const TOKEN_POST = (body: TokenPostProps) => (
  {url: baseUrl + "/jwt-auth/v1/token",
  options: {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  }
  }
)

export const TOKEN_VALIDATE_POST = (token: string) => (
  {url: baseUrl + "/jwt-auth/v1/token/validate",
  options: {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token 
    },
  }
  }
)

export const USER_GET = (token: string) => (
  {
    url: baseUrl + '/api/user', 
    options: {
      method: 'GET',
      headers: {
        Authorization: 'Bearer ' + token 
    }
  }
}
)