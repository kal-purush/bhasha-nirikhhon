import axios from "axios";

export const createFolder = async (name: string) => {
    let data = JSON.stringify({
        "name": name
    });

    let config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: 'http://localhost:8080/api/v1/folder/',
        withCredentials: true,
        headers: {
            'Content-Type': 'application/json'
        },
        data: data
    };


    try {
        const response = await axios.request(config);
        return response.data
    }
    catch (error) {
        console.log(error);
        throw new Error("Failed to create a folder")
        // error
    }

}

export const createNote = async (req: { title: string, content: string, author: string, tags: Array<string>, folderId: string }) => {
    let data = JSON.stringify({
        "title": req.title,
        "content": req.content,
        "author": req.author,
        "tags": req.tags,
        "folderId": req.folderId
    });

    let config = {
        method: 'post',
        maxBodyLength: Infinity,
        url: 'http://localhost:8080/api/v1/notes/',
        withCredentials: true,
        headers: {
            'Content-Type': 'application/json'
        },
        data: data
    };

    try {
        const response = await axios.request(config);
        return response.data
    }
    catch (error) {
        console.log(error);
        throw new Error("Failed to create note")
    }

}

export const updateNote = async (req:{title:string,content:string,author:string,tags:Array<string>},id:string) => {

    let data = JSON.stringify({
        "title": req.title,
        "content": req.content,
        "author": req.author,
        "tags": req.tags
    });

    let config = {
        method: 'put',
        maxBodyLength: Infinity,
        url: `http://localhost:8080/api/v1/notes/${id}`,
        withCredentials: true,
        headers: {
            'Content-Type': 'application/json'
        },
        data: data
    };

    try {
        const response = await axios.request(config);
        console.log(JSON.stringify(response.data));
        return response.data
    }
    catch (error) {
        console.log(error);
    }

}

export const getFolderNotes = async(id:string)=>{

let config = {
  method: 'get',
  maxBodyLength: Infinity,
  url: `http://localhost:8080/api/v1/folder/all/notes/${id}`,
  withCredentials: true,
  headers: { }
};

  try {
    const response = await axios.request(config);
    console.log(JSON.stringify(response.data));
    return response.data
  }
  catch (error) {
    console.log(error);
  }

}