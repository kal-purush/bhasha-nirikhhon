export async function get<T>(path: string, accessToken: string): Promise<T> {
  try {
    const response = await fetch(
      `${process.env.REACT_APP_SERVER_BASE_URL}${path}`,
      {
        method: 'GET',
        headers: new Headers({
          'Content-Type': 'application/json',
          authorization: `Bearer ${accessToken}`,
        }),
      }
    );
    return response.json();
  } catch (ex) {
    console.error(ex);
    return new Promise(() => ex);
  }
}

// async function post<T>(url: string, body: T) {
//   try {
//     const { getIdTokenClaims } = useAuth0();
//     const accessToken = await getIdTokenClaims();
//     const response = await fetch(
//       `${process.env.REACT_APP_SERVER_BASE_URL}/blog/post`,
//       {
//         method: 'post',
//         headers: new Headers({
//           'Content-Type': 'application/json',
//           Accept: 'application/json',
//           authorization: `Bearer ${accessToken.__raw}`,
//         }),
//         body: JSON.stringify(body),
//       }
//     );
//     return response.ok;
//   } catch (ex) {
//     return false;
//   }
// }