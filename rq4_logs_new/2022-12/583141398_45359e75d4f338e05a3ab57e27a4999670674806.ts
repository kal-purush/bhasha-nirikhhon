import { Skill } from "../../entities/skill";

export interface IDevRequest {
    name: string;
    email: string;
    password: string;
    social: string;
    avatarUrl: string;
    role: string;    
    title: string;
    level: string;
    skills: Skill[];
}

export interface IDev {
    id: string;
    name: string;
    email: string;
    password: string;
    social: string;
    avatarUrl: string;
    role: string;
    title: string;
    level: string;
    skills: Skill[];
    createdAt: Date;
    updatedAt: Date;
}

export interface IDevUpdate {
    name: string;
    email: string;
    password: string;
    social: string;
    avatarUrl: string;
    title: string;
    level: string;
    skills: Skill[];
}