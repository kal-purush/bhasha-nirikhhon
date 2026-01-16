
const baseUrl = 'https:///mesto.nomoreparties.co/v1/plus-cohort-25'
const headers = {
  'content-type': 'application/json',
  authorization: '5dc000c4-85fb-4091-8cec-e004bae4f9b4'
}

function checkResponse(res) {
    if (res.ok) {
      return res.json();
    }
    return Promise.reject(`Ошибка: ${res.status}`);
  };
  
  export function getUser(){
    return fetch(`${baseUrl}/users/me`, {
      headers
    }).then(checkResponse);
  };
  
  export function editProfileInfo(editData){
    return fetch(`${baseUrl}/users/me`, {
      method: "PATCH",
      headers,
      body: JSON.stringify(editData),
    }).then(checkResponse);
  };
  
  export function changeAvatar(editData) {
    return fetch(`${baseUrl}/users/me/avatar`, {
      method: "PATCH",
      headers,
      body: JSON.stringify(editData),
    }).then(checkResponse);
  };
  
  export function getInitialCards() {
    return fetch(`${baseUrl}/cards`, {
      headers,
    }).then(checkResponse);
  };
  
  export function addCards(inputData) {
    return fetch(`${baseUrl}/cards`, {
      method: "POST",
      headers,
      body: JSON.stringify(inputData),
    }).then(checkResponse);
  };
  
  export function deleteCards(cardID) {
    return fetch(`${baseUrl}/cards/${cardID}`, {
      method: "DELETE",
      headers,
    }).then(checkResponse);
  };
  
  export function addLike(cardID) {
    return fetch(`${baseUrl}/cards/likes/${cardID}`, {
      method: "PUT",
      headers,
    }).then(checkResponse);
  };
  
  export function removeLike (cardID) {
    return fetch(`${baseUrl}/cards/likes/${cardID}`, {
      method: "DELETE",
      headers,
    }).then(checkResponse);
  };
