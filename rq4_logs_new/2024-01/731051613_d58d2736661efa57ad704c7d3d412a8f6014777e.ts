export const NP_API =' https://api.novaposhta.ua/v2.0/json/';
const API_KEY = '9b1cc822dd4ffe5bbf944b28012dfe58';

export let OBLASTS = [];

fetch(NP_API, {
    method: "POST", 
    body: JSON.stringify({
      "apiKey": API_KEY,
      "modelName": "Address",
      "calledMethod": "getAreas",
      "methodProperties": {},
    })
  })
    .then(resp => resp.json())
    .then(data => {
      OBLASTS = data.data;
    })