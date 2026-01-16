import * as mongoose from "mongoose";
import { default as User, UserModel } from "../User/User";
import { default as JoinRequest } from "./Join-request";
import { asyncMiddleware } from "../../common/common";
import { createNotification } from "../User/Notification";
import {
    createJoinRequest,
    JoinRequestModel,
    IJoinRequest,
    joinRequestSchema,
    JoinRequestStatus
} from "./Join-request";

export enum EventType {
    event = "event",
    expenseEvent = "expense-event",
}

export type EventModel = mongoose.Document & {
    number: number
    typeNumber: number
    sharedFlatId: string
    previousExpenseId: string
    createdAt: Date
    createdBy: {
        id: string
        name: string
    }
    last: boolean
    type: EventType.event
};

export const event = {
    number: Number,
    sharedFlatId: String,
    previousExpenseId: String,
    createdAt: Date,
    createdBy: {
        id: String,
        name: String,
    },
    last: Boolean,
    type: String,
};

const eventSchema = new mongoose.Schema(event);

/**
 * Middleware assigniation
 */
eventSchema.pre("save", async function save(this: EventModel, next: Function) {
    const newEvent = await Event.findOne({ number: { $gt: this.number } });
    if (newEvent) this.last = false;
});

const Event = mongoose.model("Event", eventSchema);
export default Event;