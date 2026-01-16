import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';
import userModel from './userModel';

@Entity({ name: 'accounts' })
class authModel {
    @PrimaryGeneratedColumn()
    id: number;

    @ManyToOne(() => userModel)
    @JoinColumn({ name: 'userId' })
    userId: userModel;

    @Column({ type: 'varchar', length: 255, unique: true })
    username: string;

    @Column({ type: 'varchar', length: 255, unique: true })
    email: string;

    @Column({ type: 'varchar', length: 255 })
    password: string;
}

export default authModel;
