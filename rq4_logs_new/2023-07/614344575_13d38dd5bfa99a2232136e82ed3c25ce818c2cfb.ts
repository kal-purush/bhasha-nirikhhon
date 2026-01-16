import { request } from "../utils/axios";
import { MAIL_URL } from "../components/common/CommonConstants";

export function mailAuthenticate(email: string) {
    return request('get', `${MAIL_URL}/${email}/authenticate`, undefined);
}

export function temporaryNumberCheck(data) {
    return request("post", `${MAIL_URL}/temporaryNumberCheck`, data);
}