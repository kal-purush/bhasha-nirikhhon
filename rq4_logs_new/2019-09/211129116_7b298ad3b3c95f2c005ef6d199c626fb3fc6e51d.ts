import {
    Entity, PrimaryGeneratedColumn,
    Column, CreateDateColumn, UpdateDateColumn
} from 'typeorm';
import { Length, IsEnum } from 'class-validator';

export enum UserRole {
    ADMIN = "admin",
    GHOST = "ghost"
}

@Entity()
export class User {

    @PrimaryGeneratedColumn()
    userId: number;

    @Column({
        type: "varchar",
        unique: true,
        length: 20
    })
    @Length(4, 20)
    username: string;

    @Column({
        length: 100,
        type: "varchar"
    })
    @Length(6, 100)
    password: string;

    @Column({
        type: "datetime"
    })
    @CreateDateColumn()
    createdAt: string;

    @Column({
        type: "datetime"
    })
    @UpdateDateColumn()
    updatedAt: string;

    @Column({
        type: "enum",
        enum: UserRole,
        default: UserRole.GHOST
    })
    @IsEnum(UserRole)
    role: UserRole;
}