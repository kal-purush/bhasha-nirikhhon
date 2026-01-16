import { Column, CreateDateColumn, Entity, JoinColumn, ManyToOne, OneToMany, OneToOne, PrimaryGeneratedColumn, UpdateDateColumn } from "typeorm";
import { GroupMember } from "./groupmember.entity";
import { Task } from "./task.entity";
import { User } from "./user.entity";

@Entity()
export class Group {
    @PrimaryGeneratedColumn("uuid")
    id: string;

    @Column("varchar", {
        length: 200
    })
    name: string;

    @Column("int")
    task_perm: number;
    
    @Column("int")
    group_perm: number;

    @ManyToOne(() => User, (user) => user.createdGroups)
    creator: User;

    @OneToMany(() => GroupMember, (groupMember) => groupMember.group)
    members: GroupMember[];

    @OneToMany(() => Task, (task) => task.group)
    tasks: Task[];

    @CreateDateColumn()
    created: Date;

    @UpdateDateColumn()
    updated: Date;
}