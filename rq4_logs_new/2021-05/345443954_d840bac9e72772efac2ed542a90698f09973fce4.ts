import { User } from "../models/user";
import { UserInfo } from "../models/user-payload";
import { fetchAndParse } from "../utils/utilFunctions";
import { baseUrl, headers } from "./config";


export function userLogin(body: UserInfo): Promise<any>{
    const url = baseUrl + '/api/login';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}

export function userRegister(body: User): Promise<any>{
    const url = baseUrl + '/api/register';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}

export function adminRegister(body: User): Promise<any>{
    const url = baseUrl + '/api/admin/register';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}
