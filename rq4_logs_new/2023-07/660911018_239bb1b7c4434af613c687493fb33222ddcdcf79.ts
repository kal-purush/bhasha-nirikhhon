import axios from "axios";
import { Folder, Note, User } from "../types";
const API = axios.create({
  baseURL: `${process.env.NEXT_PUBLIC_SERVER_URL}`,
});

//Fetch Other Public notes of user on your home page (feed page)
export const fetchNotesOnFeed = async ():Promise<Note[]> => {
  const response = await API.get(`/api/v1/notes/public`, {
    withCredentials: true,
  });
  return response.data;
};

//Fetch user of note (fetch owner of note)
export const fetchUser = async (userid: string):Promise<User> => {
  const response = await API.get(`/api/v1/user/${userid}`, {
    withCredentials: true,
  });
  return response.data;
};


//Fetch user of details of logged in user
export const userDetails = async ():Promise<User>=> {
  const response = await API.get(`/auth/getuser`, {
    withCredentials: true,
  });
  return response.data;
};

//Fetch all folders of user
export const fetchAllFolders = async ():Promise<Folder[]> => {
  const response = await API.get("/api/v1/folder", { withCredentials: true });
  return response.data;
};


//Fetch all folders of user
export const creteFolder = async (name:string) => {
  const response = await API.post('/api/v1/folder', { name }, { withCredentials: true })
  return response.data;
};

//Fetch note details by note id
export const fetchNote = async (noteid: string):Promise<Note> => {
    const response = await API.get(`/api/v1/notes/${noteid}`, {
      withCredentials: true,
    });
    return response.data;
};

//Fetch note details by note id
export const updateNote = async (noteid: string, data: any) => {
  const response = await API.put(`/api/v1/notes/${noteid}`, data, {
    withCredentials: true,
  });
  return response.data;
};

//Fetch note details by note id
export const fetchAllTags = async () => {
  const response = await API.get(`/api/v1/notes/tags/all`, {
    withCredentials: true,
  });
  return response.data;
};
export default API;