export const BASE_URL = 'https://frontend-candidate.dev.sdh.com.ua/v1/contact/';

export async function getAllUsers() {
  const response = await fetch(`${BASE_URL}`);

  // eslint-disable-next-line no-console
  console.log(response);

  return response.json();
}

export async function getUser(id: number) {
  const response = await fetch(`${BASE_URL}${id}`);

  return response.json();
}

export async function delUser(id : number) {
  const response = await fetch(`${BASE_URL}${id}`, { method: 'DELETE' });

  return response;
}

export async function createUser(
  name : string,
  email : string,
  body : string,
  postId: number | undefined,
) {
  await fetch(`${BASE_URL}`, {
    method: 'POST',
    headers: {
      'Content-type': 'application/json; charset=UTF-8',
    },
    body: JSON.stringify({
      postId,
      name,
      email,
      body,
    }),
  });
}

export async function updateUser(id: number) {
  await fetch(`${BASE_URL}${id}`, {
    method: 'PUT',
    headers: {
      'Content-type': 'application/json; charset=UTF-8',
    },
    body: JSON.stringify({ }),
  });
}