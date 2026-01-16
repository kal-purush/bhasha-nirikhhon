import client from './client'

const endPoint= 'listings'
const postListings = () => client.post(endPoint)
export default {
    postListings
}