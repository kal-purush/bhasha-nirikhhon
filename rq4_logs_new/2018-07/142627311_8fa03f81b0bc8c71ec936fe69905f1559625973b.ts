import axios from 'axios';

const tokenConfig = (token: string) => ({
  headers: {
    Authorization: token
  }
});

export function validate_token(token: string) {
  return axios.post('/api/is_token_valid', {
    token
  });
}

export function get_github_access() {
  window.open(
    '/github-login',
    '_blank' // <- This is what makes it open in a new window.
  );
}

export function createUserAPI(email: string, password: string) {
  return axios.post('/api/create_user', {
    email,
    password
  });
}

export function getTokenAPI(email: string, password: string): Promise<any> {
  return axios.post('/api/get_token', {
    email,
    password
  });
}

export function has_github_token(token: string) {
  return axios.get('/api/has_github_token', tokenConfig(token));
}

export function data_about_user(token: string) {
  return axios.get('/api/user', tokenConfig(token));
}