import axios from 'axios';

export const AUTH = "AUTH";
export const PROFILES = "PROFILES";
export const S3_UPLOAD = 'S3_UPLOAD';
export const GET_USER = 'GET_USER';
export function auth(info){
  return{
    type: AUTH,
    payload: info
  };
}

export function facebookInfo(info){
  (async() => {
    console.log(info)
    // let response = await axios.create('/api/profile', info);
    // console.log(response)
  })();
}

export function getProfiles(){
      let request = axios.get('/api/profiles');
      return{
        type: PROFILES,
        payload: request
      }
  }

  export function getUserProfile(email){
          let request = axios.get('/api/profile/:email');
          return {
            type: GET_USER,
            payload: request
          }
  }