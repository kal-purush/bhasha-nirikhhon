import * as passport from "passport";

import { Request, Response, NextFunction } from "express";
import { LocalStrategyInfo } from "passport-local";

import { asyncMiddleware } from "../common/common";
import { format } from "../common/factories";

import { default as SharedFlat, SharedFlatModel } from "../models/Shared-flat/Shared-flat";
import { default as JoinRequest, JoinRequestModel } from "../models/Shared-flat/Join-request";
import { default as User, UserModel } from "../models/User/User";

/**
 * GET /shared-flat/{id}/join-request
 */
export const getJoinSharedFlatRequest =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.id) {
            return res.status(400).json(format("Missing {id} param"));
        }

        try {
            const sharedFlat = await SharedFlat.findById(req.params.id) as SharedFlatModel;
            const joinRequests = await JoinRequest.find({ sharedFlatId: { $in: [sharedFlat.id] }}) as JoinRequestModel[];
            res.status(200).json(joinRequests);
        } catch (err) {
            res.status(500).json(format(err));
        }
    });

/**
 * POST /shared-flat/{id}/join-request
 */
export const postJoinSharedFlatRequest =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.id) {
            return res.status(400).json(format("Missing {id} param"));
        }

        try {
            const sharedFlat = await SharedFlat.findById(req.params.id) as SharedFlatModel;
            await sharedFlat.makeJoinRequest(req.user);
            res.status(200).json(format("Request succesfully posted"));
        } catch (err) {
            res.status(500).json(format(err));
        }
    });

/**
 * POST /shared-flat/{sharedFlatId}/join-request/{joinRequestId}/validate
 */
export const postValidateJoinRequest =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.sharedFlatId) {
            return res.status(400).json(format("Missing {sharedFlatId} param"));
        }
        if (!req.params.joinRequestId) {
            return res.status(400).json(format("Missing {joinRequestId} param"));
        }

        try {
            const user = (req.user as UserModel);
            await user.acceptOrReject(req.params.joinRequestId, req.params.sharedFlatId, "accepted");
            res.status(200).json(format("Request succesfully validated"));
        } catch (err) {
            res.status(500).json(format(err));
        }
    });

/**
 * POST /shared-flat/{sharedFlatId}/join-request/{joinRequestId}/reject
 */
export const postRejectJoinRequest =
    asyncMiddleware(async (req: Request, res: Response, next: NextFunction) => {
        if (!req.params.sharedFlatId) {
            return res.status(400).json(format("Missing {sharedFlatId} param"));
        }
        if (!req.params.joinRequestId) {
            return res.status(400).json(format("Missing {joinRequestId} param"));
        }

        try {
            const user = (req.user as UserModel);
            await user.acceptOrReject(req.params.joinRequestId, req.params.sharedFlatId, "rejected");
            res.status(200).json(format("Request succesfully rejected"));
        } catch (err) {
            res.status(500).json(format(err));
        }
    });