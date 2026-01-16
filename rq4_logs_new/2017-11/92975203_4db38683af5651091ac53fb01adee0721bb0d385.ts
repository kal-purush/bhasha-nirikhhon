import { Request_Processor, Version, VersionPreprocessor } from 'vineyard-lawn';
import { UserService } from "./user-service";
export declare class LoggedInPreprocessor extends VersionPreprocessor {
    constructor(versions: Version[]);
    createAnonymous(): Request_Processor;
    createAuthorized(userService: UserService): Request_Processor;
}