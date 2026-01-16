import { fetchAndParse } from "../utils/utilFunctions";
import { baseUrl, headers } from "./config";
import { Favorite } from "../models/favorite";

export function addToFavorites(body: Favorite): Promise<any>{
    const url = baseUrl + '/api/favorite/add';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}

export function removeFromFavorites(body: Favorite): Promise<any>{
    const url = baseUrl + '/api/favorite/remove';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}