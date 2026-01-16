import fetchJsonp from "fetch-jsonp";

const fetchWrapper = async <T>(url: string, options: fetchJsonp.Options): Promise<T> => fetchJsonp(url,options).then(raw => raw.json<T>());

export default fetchWrapper;