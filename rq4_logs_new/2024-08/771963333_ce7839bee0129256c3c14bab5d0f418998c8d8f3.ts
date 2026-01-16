export interface LoginData {
  email: string;
  password: string;
}

export async function loginUser(data: LoginData) {
  const { email, password } = data;

  try {
    const response = await fetch('http://localhost:8080/api/v1/members/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ email, password }),
      credentials: 'include',
    });

    const responseData = await response.json();
    console.log('로그인 응답:', responseData);

    return responseData;
  } catch (error) {
    console.log('로그인 에러:', error);
  }
}