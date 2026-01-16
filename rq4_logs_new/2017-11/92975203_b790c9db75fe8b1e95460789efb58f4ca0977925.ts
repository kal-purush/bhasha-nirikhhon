import { WebClient } from "vineyard-lawn/lab";
export declare type UserIdentifier = {
    email: string;
} | {
    username: string;
} | {
    id: string;
};
export declare type CreateUserData = UserIdentifier & {
    password: string;
    twoFactorSecret?: string;
};
export declare class UserClient {
    private webClient;
    private createUserResponse;
    private password;
    private twoFactorSecret;
    private userIdentifier;
    constructor(webClient: WebClient);
    prepareTwoFactor(): Promise<string>;
    register<CreateUserResponse>(createUser: CreateUserData): Promise<CreateUserResponse>;
    login(): Promise<void>;
    loginWithUsername(): Promise<void>;
    loginWithEmail(): Promise<void>;
    logout(): Promise<void>;
    getWebClient(): WebClient;
    getUserIdentifier(): UserIdentifier;
}