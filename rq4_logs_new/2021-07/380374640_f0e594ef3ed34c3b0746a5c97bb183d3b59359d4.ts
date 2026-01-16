import { BASE_URL } from '.';
import { Issue } from '../types';

export const addIssueToChannelApi = (newIssue: Issue): Promise<Response> => {
  let token = localStorage.getItem('accessToken');
  return fetch(BASE_URL + '/channels/:id/issues', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer: ${token}`
    },
    body: JSON.stringify({ newIssue })
  });
};

export const closeIssueApi = (closedIssue: Issue): Promise<Response> => {
  let token = localStorage.getItem('accessToken');
  return fetch(BASE_URL + '/issues/:id/resolve', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer: ${token}`
    },
    body: JSON.stringify({ closedIssue })
  });
};