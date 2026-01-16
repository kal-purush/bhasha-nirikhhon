const db = require('sqlite');
import {Credentials} from "./credentials";

/**
 * Created by wojt on 26.11.16.
 */

export const extractCredentials = async (databaseFilename: string): Promise<Credentials> => {
    try {
        const database = await db.open(databaseFilename);
        const result = await db.get('SELECT * from AddonSettings WHERE key = ?', 'clientInfo');
        if (result) {
            const authData: Credentials = { addonKey: result.key, clientKey: result.clientKey };
            return authData;
        }
        else
            return null;
    }
    catch (e) {
        return null;
    }
};

export const getHttpClient = (addon, request): any => {
    let httpClient = addon.httpClient(request); // try to get httpClient in the standard way first
    if (httpClient == null) {
        httpClient = addon.httpClient(request.context);
    }
    return httpClient;
};