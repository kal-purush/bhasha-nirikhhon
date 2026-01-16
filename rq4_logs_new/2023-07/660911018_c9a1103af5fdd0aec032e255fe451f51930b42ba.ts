import axios from "axios";
const API =  axios.create({
    baseURL: `${process.env.NEXT_PUBLIC_SERVER_URL}`
})

//Fetch Other Public notes of user on your home page (feed page)
export const fetchNotesOnFeed = async () => {
    const response = await API.get(`/api/v1/notes/public`, { withCredentials: true })
    return response.data
}


//Fetch user of note (fetch owner of note)
export const fetchUser = async (userid:string) => {
    const response = await API.get(`/api/v1/user/${userid}`, { withCredentials: true })
    return response.data
}


//Fetch all folders of user
export const fetchAllFolders = async () => {
    const response = await API.get('/api/v1/folder',{withCredentials: true})
    return response.data
}


//Fetch note details by note id
export const fetchNote = async (noteid:string) => {
    const response = await API.get(`/api/v1/notes/${noteid}`,{withCredentials: true})
    return response.data
}


//Fetch note details by note id
export const updateNote = async (noteid:string,data:any) => {
    const response = await API.put(`/api/v1/notes/${noteid}`,data,{withCredentials: true})
    return response.data
}
export default API