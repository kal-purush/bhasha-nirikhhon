import { useQuery } from '@tanstack/react-query'
import axios from 'axios'

const fetchPosts = async (limit = 10) => {
  const parsed = (await axios.get('https://jsonplaceholder.typicode.com/posts')).data
  const result = parsed.filter((x) => x.id <= limit)
  return result
}

const usePosts = (limit) => {
  return useQuery(['posts', limit], () => fetchPosts(limit))
}

export { usePosts, fetchPosts }