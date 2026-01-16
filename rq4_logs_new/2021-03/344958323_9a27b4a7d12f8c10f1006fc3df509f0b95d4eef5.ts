export type User = {
    id: number;
    name: string;
    login: string;
    password: string;
    email: string;
    status: boolean;
}

export type Role = {
    id: number;
    authority: string;
}