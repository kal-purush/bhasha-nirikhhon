import IRatingDTO from "@modules/providers/dtos/IRatingDTO";
import IRatingRepository from "@modules/providers/repositories/IRatingRepository";
import { getRepository, Repository } from "typeorm";
import Rating from "../entities/Rating";

export default class RatingRepository implements IRatingRepository{
    private ormRepository : Repository<Rating>;

    constructor(){
        this.ormRepository = getRepository(Rating);
    }
    public async create(ratingDTO: IRatingDTO): Promise<Rating> {
        const rating = await this.ormRepository.create(ratingDTO);
        await this.ormRepository.save(ratingDTO);
        return rating;
    }
    public async getRatingAverage(id: string): Promise<number>{
        const ratings = await this.ormRepository.find({
            where: {provider: {id: id}}
        });
        let average = 0;
        for (let i = 0; i < ratings.length; i++){
            average += ratings[i].rating_number;
        }
        return Math.floor(average/ratings.length);
    }
}