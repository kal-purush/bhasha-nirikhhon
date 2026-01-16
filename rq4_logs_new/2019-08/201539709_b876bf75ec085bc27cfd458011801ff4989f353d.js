import {useEffect, useState} from 'react';
import {SERVER_URL} from './config';

const options = {};

export async function deleteResource(urlSuffix) {
  const url = SERVER_URL + urlSuffix;
  await fetch(url, {method: 'DELETE'});
}

export async function getJson(urlSuffix) {
  const url = SERVER_URL + urlSuffix;
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export async function getText(urlSuffix) {
  const url = SERVER_URL + urlSuffix;
  const res = await fetch(url, options);
  const text = await res.text();
  if (!res.ok) throw new Error(text || res.statusText);
  return text;
}

export function postJson(urlSuffix, obj) {
  return postPutJson('POST', urlSuffix, obj);
}

export function putJson(urlSuffix, obj) {
  return postPutJson('PUT', urlSuffix, obj);
}

async function postPutJson(method, urlSuffix, obj) {
  const body = JSON.stringify(obj);
  const headers = {'Content-Type': 'application/json'};
  const url = SERVER_URL + urlSuffix;
  const res = await fetch(url, {...options, method, headers, body});
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export function useFetchState(urlSuffix, initialValue) {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    getJson(urlSuffix)
      .then(setValue)
      .catch(e => console.error(e));
  }, [urlSuffix]);

  return [value, setValue];
}