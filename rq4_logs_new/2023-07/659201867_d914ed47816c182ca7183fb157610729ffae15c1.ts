import { DeleteResult } from "typeorm";
import { Like } from "../../database/entities/Like";
import likeRepository from "../../database/repository/like.repository";
import { ILikeHandler } from "./interface/ilike.handler";


class VoteHandler implements ILikeHandler {
    async create(data: Like): Promise<Like> {
        try {
            const newVote = await likeRepository.create(data);
            return newVote
        } catch (error) {
            throw new Error(error)
        }
    }

    async delete(id: number): Promise<DeleteResult> {
        try {
            const deletedVote = await likeRepository.delete(id)
            return deletedVote;
        } catch (error) {
            throw new Error(error)
        } 
    }
}

export default new VoteHandler();