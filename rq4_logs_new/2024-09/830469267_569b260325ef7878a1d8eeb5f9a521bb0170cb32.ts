import { Request, Response } from "express";

import { Comment } from "../../models/Comment";

const deleteComment = async (req: Request, res: Response) => {
    const commentId: number = parseInt(req.params.commentId);

    if (!commentId) {
        return res.status(400).json({ message: "Invalid commentId" });
    }

    try {
        await Comment.destroy({ where: { id: commentId } });

        res.status(200).json({ message: "Comment deleted successfully", error: false });
    } catch (e: any) {
        console.log("Failed to delete comment", e);
        res.status(500).json({
            message: "An error has occured while deleting comment",
            error: true,
        });
    }
};

export default deleteComment;