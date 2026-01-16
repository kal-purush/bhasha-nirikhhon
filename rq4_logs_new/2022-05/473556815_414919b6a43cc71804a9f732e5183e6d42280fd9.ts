import {ProfileRepository} from "../repository/ProfileRepository";
import {initializeApp} from "firebase/app";
import {config} from "../../../firebase_config";

export class Assigner {
    private readonly user_repository: ProfileRepository;

    constructor(user_repo: ProfileRepository) {
        this.user_repository = user_repo;
        initializeApp(config);
    }

    async isFlatMate(uid: string): Promise<boolean> {
        const req_user = await this.user_repository.getProfileById(uid);
        return req_user.flatId != "";
    }
}