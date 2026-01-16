import * as passport from "passport";
import * as R from "ramda";

import { Request, Response, NextFunction } from "express";
import { LocalStrategyInfo } from "passport-local";

import { asyncMiddleware } from "../common/common";
import { format } from "../common/factories";

import { default as Event, EventModel, EventType, createEvent } from "../models/Shared-flat/Event";
import { default as SharedFlat, SharedFlatModel } from "../models/Shared-flat/Shared-flat";
import { default as JoinRequest, JoinRequestModel } from "../models/Shared-flat/Join-request";
import { default as User, UserModel } from "../models/User/User";

/**
 * GET /shared-flat/{id}/event
 */
export const getEventList =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.id) {
            return res.status(400).json(format("Missing {id} param"));
        }

        try {
            const sharedFlat = await SharedFlat.findById(req.params.id) as SharedFlatModel;
            if (undefined == sharedFlat) throw new Error(`Shared flat with id {${req.params.id}} not found`);
            if (!sharedFlat.isMember(req.user)) throw new Error("Only shared flat resident should see events");

            let filters = { sharedFlatId: sharedFlat.id};
            if (req.params.eventType) {
                filters = R.merge(filters, { eventType: req.params.eventType });
            }

            const events = await Event.find(filters, {}, { sort: { number: -1 }}) as EventModel[];
            res.status(200).json(events);
        } catch (err) {
            res.status(500).json(format(err));
        }
    });

/**
 * POST /shared-flat/{id}/event
 */
export const postEvent =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.id) {
            return res.status(400).json(format("Missing {id} param"));
        }

        // @todo check amount param validity
        const amount = req.params.amount || 0;
        const eventType = req.params.eventType === EventType.event ? EventType.event : EventType.expenseEvent;

        try {
            const sharedFlat = await SharedFlat.findById(req.params.id) as SharedFlatModel;
            if (undefined == sharedFlat) throw new Error(`Shared flat with id {${req.params.id}} not found`);

            const user = await User.findById(req.user.id) as UserModel;
            if (!sharedFlat.isMember(user)) throw new Error("Only shared flat resident should see events");

            const previousEvent = await Event.findOne({}, {}, { sort: { createdAt: -1 }}) as EventModel;
            const event = new Event(createEvent(
                sharedFlat,
                eventType,
                user,
                amount,
                previousEvent,
            ));
            console.log(event);

            await event.save();
            res.status(201).json(format("Event created"));
        } catch (err) {
            res.status(500).json(format(err));
        }
    });