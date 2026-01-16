import {IWebService} from "../services/IWebService";
import AxiosWebService from "../services/AxiosWebService";

/**
 * Function to get current implementation of Web Request Service
 */
export function getRequestService(): IWebService {
    return AxiosWebService()
}