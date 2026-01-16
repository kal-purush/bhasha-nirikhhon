function callApi_(path) {
  if (path.startsWith('/player/')) {
    return MSF.callApi(path);
  }

  const token = getAccessToken_();

  if (!token) {
    return false;
  }

  const params = {
    method: 'get',
    headers: {
      'x-api-key': API_KEY,
      Authorization: `Bearer ${token}`
    },
    muteHttpExncuceptions: true
  };
  const url = API_BASE + path;
  const response = UrlFetchApp.fetch(url, params);

  if (response.getResponseCode() === 403) {
    console.log(response.getContentText());
    return false;
  }
  return response;
}

function getAccessToken_() {
  const params = {
    method: 'post',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${Utilities.base64Encode(`${CLIENT_ID}:${CLIENT_SECRET}`)}`
    },
    payload: { grant_type: 'client_credentials' },
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(AUTH_URL, params);
  if (response.getResponseCode() != 200) {
    return false;
  }

  const data = response.getContentText();
  return JSON.parse(data).access_token;
}