import { Request, Response } from "express";
import { createPostService,getAllPostsService, likePostService } from "../services/posts/createPost.service";
import AppError from "../errors";

const createPostController = async (req: Request, res: Response): Promise<Response> => {
    if (!req.files) {
        throw new AppError("upload falhou!");
    }
    const imgsLocal = (req.files as Express.Multer.File[]).map((file) => file.filename);
    const newPost = await createPostService(req.body, res.locals.userAuth.id, imgsLocal);
    return res.status(201).json(newPost);
}
const getAllPostsController = async (_req: Request, res: Response): Promise<Response> => {
    const allPosts = await getAllPostsService();

    return res.status(200).json(allPosts);
}
const likePost = async (req: Request, res: Response): Promise<Response> => {

    await likePostService(Number(req.params.postId), res.locals.userAuth.id);

    return res.status(200);
}

export {
    createPostController,
    getAllPostsController,
    likePost
}