import { Review } from "../models/review";
import {ReviewPayload } from "../models/review-payload";
import { fetchAndParse } from "../utils/utilFunctions";
import { baseUrl, headers } from "./config";


export function getReviews(id: string): Promise<any>{
    const url = baseUrl + `/api/reviews/${id}`;
    return fetchAndParse<any>(url, {method: "GET", headers})
}

export function uploadReview(body: ReviewPayload): Promise<Review>{
    const url = baseUrl + '/api/reviews';
    return fetchAndParse<any>(url, {method: "POST",body: JSON.stringify(body), headers})
}