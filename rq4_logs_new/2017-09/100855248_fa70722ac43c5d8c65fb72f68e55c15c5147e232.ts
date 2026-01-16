import { UserModel } from "../models/User";

export type NotificationType = "alert" | "success" | "info";
export interface INotification {
    message: string;
    type: NotificationType;
    createdAt: Date;

    readed: boolean;
}

export const createNotification = (message: string, type: NotificationType): INotification =>
    ({ message, type, createdAt: new Date(), readed: false });


export const createResponse = (message: string) => ({ message });


export type JoinRequestStatus = "pending" | "accepted" | "rejected";

export type JoinRequest = {
    id: string
    doAt: Date
    status: JoinRequestStatus
};
export const createJoinRequest = (resident: UserModel): JoinRequest => ({
    id: resident.id,
    status: "pending",
    doAt: new Date(),
});