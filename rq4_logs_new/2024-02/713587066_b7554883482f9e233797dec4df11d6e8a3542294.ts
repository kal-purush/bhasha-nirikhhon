import { api } from '../api'

async function getUsers() {
  const response = await api.get('users/')
  return response.data
}

const usersQuery = {
  getUsers,
}

export default usersQuery