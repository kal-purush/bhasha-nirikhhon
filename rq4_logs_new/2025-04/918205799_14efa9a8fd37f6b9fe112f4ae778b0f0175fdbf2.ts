import {Column, CreateDateColumn, Entity, ManyToOne, PrimaryGeneratedColumn, UpdateDateColumn} from "typeorm";
import Users from "@root/entity/Users";

export const POST_TABLE_NAME = "post"
@Entity({name : POST_TABLE_NAME})
export default class Post {
    @PrimaryGeneratedColumn({type: "bigint"})
    id: number;

    @ManyToOne(() => Users, {onDelete: "CASCADE"})
    user: Users;

    @Column()
    title: string;

    @Column()
    body: string;

    @CreateDateColumn()
    createdAt: Date

    @UpdateDateColumn()
    updatedAt: Date
}