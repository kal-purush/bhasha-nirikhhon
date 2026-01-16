import { IsEmail } from 'class-validator';
import {
    BaseEntity,
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    UpdateDateColumn
} from 'typeorm';
import { createDeflateRaw } from 'zlib';

@Entity()
class User extends BaseEntity {
    @PrimaryGeneratedColumn()
    id: number;

    @Column({ type: 'text', unique: true })
    @IsEmail()
    email: string;

    @Column({ type: 'boolean', default: false })
    verifiedEmail: boolean;

    @Column({ type: 'text' })
    firstName: string;

    @Column({ type: 'text' })
    lastName: string;

    @Column({ type: 'int' })
    age: number;

    @Column({ type: 'text' })
    password: string;

    @Column({ type: 'text' })
    phoneNumber: string;

    @Column({ type: 'boolean', default: false })
    verifiedPhoneNumber: boolean;

    @Column({ type: 'text', default: false })
    profilePhoto: string;

    @CreateDateColumn()
    createdAt: string;

    @UpdateDateColumn()
    updatedAt: string;
}

export default User;