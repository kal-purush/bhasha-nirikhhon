import GenericResponse from "../model/dto/GenericResponse";
import Notification, {NotificationType} from "../model/Notification";
import CommonResponse from "../model/dto/CommonResponse";

function getAllNotifications(userId: string) {
    //TODO: implement
    return new Promise<GenericResponse<Notification[]>>(
        (res, rej) => res(new GenericResponse([new Notification(
             'BlaBla', true, NotificationType.APPLIANCE, '1')])));
}

function sendNotification(notification: Notification) {
    //TODO: implement
    return new Promise<CommonResponse>((res, rej) => res(new CommonResponse()));
}