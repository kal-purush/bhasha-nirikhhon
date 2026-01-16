import { randomUUID } from 'crypto';
import { Entity, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, PrimaryColumn } from 'typeorm';
import { User } from '../../users/domain/user.typeorm.entity';

@Entity('Sessions')
export class Session {
@PrimaryGeneratedColumn('uuid')
id: string;

@Column({ type: 'uuid', nullable: false })
user_id: string;

@ManyToOne(() => User)
@JoinColumn({ name: 'user_id' })
user: User;

@Column({ type: 'uuid', nullable: false })
device_id: string;

@Column({ type: 'varchar', nullable: false })
iat: string;

@Column({ type: 'varchar', nullable: false })
exp: string;

@Column({ type: 'varchar', nullable: false })
device_name: string;

@Column({ type: 'varchar', nullable: false })
ip: string;

static createSession(userId: string, deviceId: string, iat: string, exp: string, userAgent: string, ip: string): Session {
    const session = new Session();
    
    session.id = randomUUID();
    session.user_id = userId;
    session.device_id = deviceId;
    session.iat = iat;
    session.exp = exp;
    session.device_name = userAgent;
    session.ip = ip;
    return session;
}
}
