import * as R from "ramda";
import { Document } from "mongoose";
import { UserModel } from "../models/User/User";
import {
    default as Event,
    EventModel,
    EventType,
    IEvent,
    IExpenseEvent,
    INeedEvent
} from "../models/Shared-flat/Event";

import {
    // default as SharedFlat,
    SharedFlatModel
} from "../models/Shared-flat/Shared-flat";

export default abstract class EventFactory {
    static async create(
        sharedFlat: SharedFlatModel,
        type: EventType,
        createdBy: UserModel,
        specificProps: any = {}
    ): Promise<Document> {

        const previousEvents = await sharedFlat.getLastEvents(createdBy.id);
        const previousEvent = R.head(previousEvents) as EventModel;

        let number: number;
        let previousExpenseId: string;

        if (!previousEvent) {
            number = 0;
            previousExpenseId = undefined;
        } else {
            number++;
            previousExpenseId = previousEvent.id;
            previousEvent.last = false;
        }

        let event: IEvent = {
            number,
            type,
            last: true,
            sharedFlatId: sharedFlat.id,
            createdBy: createdBy.id,
            createdAt: new Date(),
            previousExpenseId,
        };

        switch (type) {
            case EventType.expenseEvent:
                if (!R.has("amount", specificProps)) {
                    throw new Error(`{amount} props is missing to instantiate an {${type}}`);
                }

                const { amount } = specificProps;

                let totalAmountAtThisTime = R.pathOr(0, ["totalAmountAtThisTime"], previousEvent);
                totalAmountAtThisTime += amount;

                const expenseProps = { amount, totalAmountAtThisTime };
                event = R.merge(event, expenseProps);
                break;

            case EventType.needEvent:
                if (!R.has("message", specificProps)) {
                    throw new Error(`{message} props is missing to instantiate an {${type}}`);
                }

                const { message } = specificProps;
                const needProps: any = { status: "pending", message };

                if (R.has("requestedResident", specificProps)) {
                    needProps.requestedResident = specificProps.requestedResident;
                }

                event = R.merge(event, needProps);
                break;

            default:
                break;
        }

        const buildEvent = new Event(event);
        const notification = `New ${event.type} created by ${createdBy.profile.name}`;

        await Promise.all([
            previousEvent.save(),
            buildEvent.save(),
            sharedFlat.notify(notification, "info"),
        ]);

        return buildEvent;
    }
}