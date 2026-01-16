export type RequestUser = {
    _id: number;
    name: number;
    email: string;
    password: string;
    email_verified: boolean;
    auth_level: number
    team?: number;
};

export type Team = {
    _id: string;
    name : string;
    creator: string;
}