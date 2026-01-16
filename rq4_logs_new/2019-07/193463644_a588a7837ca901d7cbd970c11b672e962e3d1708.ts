import { Dependencies } from "../types";
import { Repository } from "typeorm";
import { Post } from "../entities/post";
// Just a middleman currently?
export class PostService {
    repository: Repository<Post>;

    constructor(options : Dependencies) {
        this.repository = options.postRepository;
    }

    find() {
        return this.repository.find();
    }

    findOne(id: number) {
        return this.repository.findOne(id);
    }

    save(post : Post) {
        this.repository.save(post);
    }

    delete(id: number) {
        return this.repository.delete(id);
    }

    update(id: number, obj: Object) {
        return this.repository.update(id, obj);
    }
}