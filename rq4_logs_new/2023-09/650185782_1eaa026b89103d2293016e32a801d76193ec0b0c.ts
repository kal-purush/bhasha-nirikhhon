/* eslint-disable prettier/prettier */
import { Initiative } from "src/initiative/initiative.entity";
import { User } from "src/users/user.entity";
import { Entity, JoinColumn, ManyToOne, PrimaryColumn } from "typeorm";;

@Entity()
export class Like {
    @PrimaryColumn()
    userId: string;
  
    @PrimaryColumn()
    initiativeId: string;
  
    @ManyToOne(() => User)
    @JoinColumn({ name: 'userId' })
    user: User;
  
    @ManyToOne(() => Initiative)
    @JoinColumn({ name: 'initiativeId' })
    initiative: Initiative;
}