import {ContentResult} from "../../View/ContentResult";

/**
 * Autotimeout requests after some time
 */
export class TimeoutRequests {

    public static TIME_TO_TIMEOUT = 5000;

    private send;
    private timer;

    before(request, response, next, send) {
        this.send = send;
        this.timer = setTimeout(() => {
            response.statusCode = 408;
            send(new ContentResult("Timeouted"));
        }, TimeoutRequests.TIME_TO_TIMEOUT);

        next();
    }

    after(request, response, send) {
        clearTimeout(this.timer);
        send();
    }

}