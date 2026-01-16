import * as mongoose from "mongoose";
import { UserModel } from "../User/User";
import { SharedFlatModel } from "../Shared-flat/Shared-flat";

export type JoinRequestStatus = "pending" | "accepted" | "rejected";

export interface IJoinRequest {
    userId: string;
    sharedFlatId: string;
    doAt: Date;
    status: JoinRequestStatus;
}

export const createJoinRequest = (resident: UserModel, sharedFlat: SharedFlatModel): IJoinRequest => ({
    userId: resident.id,
    sharedFlatId: sharedFlat.id,
    status: "pending",
    doAt: new Date(),
});

export type JoinRequestModel = mongoose.Document & IJoinRequest & {
    validateRequest: () => Promise<void>
    refuseRequest: () => Promise<void>
};

export const joinRequestSchema = new mongoose.Schema({
    userId: String,
    sharedFlatId: String,
    doAt: { type: Date, default: Date.now },
    status: { type: String, default: "pending" },
});

joinRequestSchema.methods.validateRequest = function (this: JoinRequestModel): Promise<void> {
    return new Promise((resolve, reject) => {
        this.status = "accepted";
        this.save((err: any) => {
            if (err) reject(err);
            resolve();
        });
    });
};

joinRequestSchema.methods.refuseRequest = function (this: JoinRequestModel): Promise<void> {
    return new Promise((resolve, reject) => {
        this.status = "rejected";
        this.save((err: any) => {
            if (err) reject(err);
            resolve();
        });
    });
};

const JoinRequest = mongoose.model("JoinRequest", joinRequestSchema);
export default JoinRequest;