import { fetchAndParse } from "../utils/utilFunctions";
import { baseUrl, headers } from "./config";


export function getArticles(email: string): Promise<any>{
    const url = baseUrl + `/api/articles/${email}`;
    return fetchAndParse<any>(url, {method: "POST", headers})
}