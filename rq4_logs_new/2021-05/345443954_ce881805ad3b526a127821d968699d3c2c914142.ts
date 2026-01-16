import { fetchAndParse } from "../utils/utilFunctions";
import { headers } from "./config";

export function uploadImage(title: string, picture: File): Promise<string>{
    const localHeaders = { ...headers,'Accept': '*/*', 'Content-Type': 'application/x-www-form-urlencoded'};
    const url = "" ;
    const data = new FormData();
    data.append('file', picture,title);
    // data.append('upload_preset', cloudinaryPreset);
    return fetchAndParse<string>(url, { method: 'POST', body: data, headers: localHeaders});
}