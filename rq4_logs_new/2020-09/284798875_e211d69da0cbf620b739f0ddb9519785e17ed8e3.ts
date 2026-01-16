import { BaseEntity } from "typeorm";
export declare class Admins extends BaseEntity {
    id: String;
    name: string;
    email: string;
    password: string;
    can_answer: boolean;
    can_users: boolean;
    can_reply: boolean;
    can_chatMod: boolean;
    created_at: Date;
    updated_at: Date;
    addId(): void;
    setPassword(): Promise<void>;
}