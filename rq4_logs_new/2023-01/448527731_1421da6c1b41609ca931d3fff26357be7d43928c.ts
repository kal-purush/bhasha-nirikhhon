import { 
    Column, 
    CreateDateColumn, 
    Entity, 
    ManyToOne, 
    PrimaryGeneratedColumn, 
    UpdateDateColumn 
} from 'typeorm';

import { UserEntity } from './user.entity';

@Entity('comments')
export class CommentEntity {
    @PrimaryGeneratedColumn()
    id: number;

    @ManyToOne(() => UserEntity) 
    author: UserEntity;

    @Column()
    type: string;

    @Column()
    objectId: number;

    @Column()
    text: string;

    @CreateDateColumn()
    createdAt: Date;

    @UpdateDateColumn()
    updatedAt: Date;
}