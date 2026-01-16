import { Request, Response } from "express";

import commentSchema from "./validationSchema";
import { Comment } from "../../models/Comment";

import { createJoiError } from "../../lib/utils";

type CommentType = {
    text: string;
    projectId: number;
    userId: number;
};

const addComment = async (req: Request, res: Response) => {
    const comment: CommentType = req.body;

    const projectId: number = parseInt(req.params.projectId);

    if (!projectId) {
        return res.status(400).json({ message: "Invalid projectId" });
    }

    try {
        const validatedComment = await commentSchema.validateAsync(comment);

        const createdComment = await Comment.create({ ...validatedComment, projectId });

        res.status(200).json({ comment: createdComment, error: false });
    } catch (e: any) {
        console.log("Failed to add comment", e);
        res.status(500).json({
            message: createJoiError(e) || "An error has occured while adding comment",
            error: true,
        });
    }
};

export default addComment;