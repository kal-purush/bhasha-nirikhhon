import { User } from "app/user";

export class Tweet {

    id: number;
    posterId: number;
    taggedUsers: User[];
    tags: string[];
    message: string;
    tweetDate: Date;
    rating: number;

    constructor(values: Object = {}) {
        Object.assign(this, values);
    }
}