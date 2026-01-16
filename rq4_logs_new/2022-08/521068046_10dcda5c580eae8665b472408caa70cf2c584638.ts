import axios from "axios";


const instance = axios.create({
  withCredentials: false,
  baseURL: process.env.REACT_APP_API_URL,
  headers: {
    Accept: "application/json",
	  "Content-Type": "application/json",
  },
});

export default instance;

export async function get(url:string, params:any) {
  const response = await instance({
    method: "GET",
    url: url,
    params: params,
  });
  return response;
}


export async function post(url:string, data:any) {
	const response = await instance({
		method: 'POST',
		url,
		data
	});

	return response;
}

export async function put(url: string, data: any) {
	const response = await instance({
		method: 'PUT',
		url,
		data
	});

	return response;
}

export async function patch(url: string, data: any) {
	const response = await instance({
		method: 'PATCH',
		url,
		data
	});

	return response;
}

export async function del(url: string) {
	const response = await instance({
		method: 'DELETE',
		url
	});

	return response;
}

export async function upload(url: string, data: any, config: []) {
	const response = await instance({
		method: 'POST',
		url,
		data: data.formData,
		...config
	});

	return response;
}