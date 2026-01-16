import { NextFunction, Request, Response, Router } from "express";
import { StatusMessages } from "../utils/status.message.enum";
import { StatusCodes } from "../utils/status.code.enum";
import CommentService from "../service/comment.service";
import ResponseBody from "../utils/response.body";
import Logger from "../logger/logger.singleton";
import RequestWithLogger from "../utils/request.logger";
import authenticate from "../middleware/authenticate.middleware";
import validateMiddleware from "../middleware/validate.middleware";
import CreateCommentDto from "../dto/create-comment.dto";
import PatchCommentDto from "../dto/patch-comment-dto";

export default class CommentController {
    public router: Router;

    constructor(private commentService: CommentService) {
        this.router = Router({ mergeParams: true });

        this.router.get("/", this.getAllComments);
        this.router.get("/:id", this.getCommentById);
        this.router.post(
            "/",
            authenticate,
            validateMiddleware(CreateCommentDto),
            this.createComment
        );
        this.router.patch(
            "/:id",
            authenticate,
            validateMiddleware(PatchCommentDto, {
                skipMissingProperties: true,
            }),
            this.patchComment
        );
        this.router.delete("/:id", this.removeComment);
    }

    getAllComments = async (req: Request, res: Response) => {
        const comments = await this.commentService.getAllComments();
        const responseBody = new ResponseBody(
            comments,
            null,
            StatusMessages.OK
        );
        responseBody.set_meta(comments.length);
        res.status(StatusCodes.OK).send(responseBody);
    };

    getCommentById = async (
        req: Request,
        res: Response,
        next: NextFunction
    ) => {
        try {
            const commentId = req.params.id;
            const comment = await this.commentService.getCommentById(
                +commentId
            );
            const responseBody = new ResponseBody(
                comment,
                null,
                StatusMessages.OK
            );
            responseBody.set_meta(1);
            res.status(StatusCodes.OK).send(responseBody);
        } catch (error) {
            next(error);
        }
    };

    createComment = async (
        req: RequestWithLogger,
        res: Response,
        next: NextFunction
    ) => {
        try {
            const comment = await this.commentService.createComment(
                req.dto,
                req.email,
                req.params.id
            );
            console.log(req.params.id);
            const responseBody = new ResponseBody(
                comment,
                null,
                StatusMessages.CREATED
            );
            responseBody.set_meta(1);
            res.status(StatusCodes.CREATED).send(responseBody);
            Logger.getLogger().log({
                level: "info",
                message: `Comment Created (${comment.id})`,
                label: req.req_id,
            });
        } catch (err) {
            next(err);
        }
    };

    patchComment = async (
        req: RequestWithLogger,
        res: Response,
        next: NextFunction
    ) => {
        let commentId = req.params.id;
        try {
            const comment = await this.commentService.patchComment(
                commentId,
                req.dto
            );
            const responseBody = new ResponseBody(
                comment,
                null,
                StatusMessages.OK
            );
            responseBody.set_meta(1);
            res.status(StatusCodes.OK).send(responseBody);
        } catch (err) {
            next(err);
        }
    };

    removeComment = async (
        req: RequestWithLogger,
        res: Response,
        next: NextFunction
    ) => {
        const commentId = +req.params.id;
        try {
            const comment = await this.commentService.removeComment(commentId);
            Logger.getLogger().log({
                level: "info",
                message: `Comment Deleted (${commentId})`,
                label: req.req_id,
            });
            res.status(StatusCodes.NO_CONTENT).send();
        } catch (err) {
            next(err);
        }
    };
}