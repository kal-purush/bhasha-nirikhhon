import { ApolloError } from "apollo-server-errors";
import axios from "axios";

export const RequestPage = async (url: string) => {
    const regexHTTP =
        /https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,4}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)/g;
    if (!url || !regexHTTP.test(url)) throw new ApolloError("URL not valid", "URL_NOT_VALID");

    if (url.endsWith('/')) {
        url = url.substring(0, url.length - 1);
    }

    const startTime = Date.now();
    return await axios
        .get(url)
        .then((res: any) => {
            const endTime = Date.now();
            const responseTime = endTime - startTime;
            return {
                status: res.status,
                responseUrl: res.request.res.responseUrl,
                redirectCount: res.request._redirectable._redirectCount,
                responseTime,
            };
        })
        .catch((err: any) => {
            const endTime = Date.now();
            const responseTime = endTime - startTime;
            console.log(err.message);
            return {
                status: err.response.status,
                responseUrl: err.request.res.responseUrl,
                redirectCount: err.request._redirectable._redirectCount,
                responseTime,
            };
        });
}