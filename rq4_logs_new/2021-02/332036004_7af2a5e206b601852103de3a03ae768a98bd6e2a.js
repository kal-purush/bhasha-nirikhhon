'use strict';

const { JSDOM } = require('jsdom');
const fetch = require('node-fetch');
const FormData = require('form-data');
const axios = require('axios');
const { moly } = require('../../config');

const checkMark = '\u2713';

async function getAuthenticityToken() {
  //TODO: error handling
  const res = await axios.get(moly.baseUrl + '/belepes');
  const sessionCookie = res.headers['set-cookie'][0];
  const { document } = (new JSDOM(res.data)).window;

  const authInputNode = document.querySelector('[name=authenticity_token]');
  const authenticityToken = authInputNode.value;

  return { authenticityToken, sessionCookie };
}

async function getUserCredentials() {
  //TODO: error handling
  const { authenticityToken, sessionCookie } = await getAuthenticityToken();
  console.log(sessionCookie);

  const form = {
    utf8: checkMark,
    authenticity_token: authenticityToken,
    'user_session[email]': moly.sffVektorUsername,
    'user_session[password]': moly.sffVektorPassword,
    'user_session[remember_me]': 0,
    commit: 'Belépés'
  };
  const params = new URLSearchParams(form);
  const queryParams = params.toString();

  const res = await axios.post(moly.baseUrl + '/azonositas', queryParams, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Cookie: sessionCookie
    }
  });
  console.log(res);
 

  /*const form = new FormData();
  form.append('authenticity_token', authenticityToken);
  form.append('user_session[email]', 'sadness');
  form.append('user_session[password]', '12345');
  form.append('commit', 'Belépés');

  const res = await fetch(moly.baseUrl + '/azonositas', {
    method: 'POST',
    body: form,
    headers: { 
      //'Content-Type': 'multipart/form-data',
      'Content-Type': `multipart/form-data; boundary=${form._boundary}`,
      //...form.getHeaders(),
      Cookie: sessionCookie
    }
  });
  console.log(res.headers.raw()['set-cookie']);
  const body = await res.text();
  console.log(body);*/
}

module.exports = {
  getAuthenticityToken,
  getUserCredentials
};