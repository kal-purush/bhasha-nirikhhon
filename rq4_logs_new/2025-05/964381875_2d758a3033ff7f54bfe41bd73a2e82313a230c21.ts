// 活動資料表
// 補: 活動與ActivitySite跟Showtimes會有一對多的關係

import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
  ManyToOne, // 如果 organizer_id 和 category_id 是實際的關聯外鍵
  JoinColumn,
  OneToMany, // 如果有其他表與此表形成一對多關聯，例如活動的場次
} from 'typeorm';
import { ActivityTypeEntity } from './ActivityType'
import { OrganizerEntity } from './Organizer'
import { ActivitySiteEntity } from './ActivitySite';
import { ShowtimesEntity } from './Showtimes';
import { ShowtimeSectionsEntity } from './ShowtimeSections';

import { DbEntity } from '../constants/dbEntity';


@Entity(DbEntity.Activity)
export class ActivityEntity {
    @PrimaryGeneratedColumn()
    id!: number;

    @Column({ type: 'varchar', length: 100, nullable: false })
    name!: string;

    @Column({ type: 'uuid', nullable: false })
    organizer_id!: string;

    @Column({ type: 'int', nullable: false })
    category_id!: number;

    @Column({ type: 'int', default: 1, nullable: false })
    status!: number;

    @Column({ type: 'varchar', length: 500, nullable: false })
    description!: string;

    @Column({ type: 'varchar', length: 1000, nullable: true })
    information?: string;

    @Column({ type: 'timestamp', nullable: false })
    start_time!: Date;

    @Column({ type: 'timestamp', nullable: false })
    end_time!: Date;

    @Column({ type: 'timestamp', nullable: false })
    sales_start_time!: Date;

    @Column({ type: 'timestamp', nullable: false })
    sales_end_time!: Date;

    @Column({ type: 'text', nullable: false })
    cover_image!: string;

    @Column({ type: 'text', nullable: true })
    banner_image?: string;
    
    @Column({ type: 'varchar', array: true, nullable: true })
    tags?: string[];

    @CreateDateColumn({ type: 'timestamp' })
    @Index()
    created_at!: Date;

    @UpdateDateColumn({ type: 'timestamp' })
    updated_at!: Date;

    @Column({ type: 'boolean', default: false })
    is_deleted!: boolean;

    // 關聯至 Organizer
    @ManyToOne(() => OrganizerEntity,  organizer => organizer.activity)
    @JoinColumn({ name: 'organizer_id', referencedColumnName: 'id' })
    organizer!: OrganizerEntity;
    
    // 關聯至 ActivityType
    @ManyToOne(() => ActivityTypeEntity)
    @JoinColumn({ name: 'category_id', referencedColumnName: 'id' })
    category!: ActivityTypeEntity;

    // 🔁 多場地活動關聯
    @OneToMany(() => ActivitySiteEntity, site => site.activity)
    sites!: ActivitySiteEntity[];

    // 🔁 多場次活動關聯
    @OneToMany(() => ShowtimesEntity, showtime => showtime.activity)
    showtimes!: ShowtimesEntity[];

    @OneToMany(() => ShowtimeSectionsEntity, section => section.activity)
    showtimeSections!: ShowtimeSectionsEntity[];

}


