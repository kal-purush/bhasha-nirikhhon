const sendLogin = ({id, pw}: {id: string, pw: string}) => {
    if (id === 'artist'&& pw==='artist') {
        return {token: 'artist-token'}
    }
    return {token: 'model-token'}
}

export const login = async (id:string, pw: string) => {
    const { token } = await sendLogin({id, pw});
    localStorage.setItem('token', token);
}

export const logout = () => {
    localStorage.removeItem('token');
}

export type UserRole = 'guest' | 'model' | 'artist';

export const getUserRole = (): UserRole => {
  const token = localStorage.getItem('token')
  if (token === 'model-token') {
    return 'model';
  }
  if (token === 'artist-token') {
    return 'artist';
  }
  return 'guest';
}