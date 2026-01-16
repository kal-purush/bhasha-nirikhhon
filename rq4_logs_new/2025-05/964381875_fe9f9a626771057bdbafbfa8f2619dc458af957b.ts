// src/entities/DailySequence.ts
import { Entity, PrimaryColumn, Column, UpdateDateColumn } from 'typeorm';
import { DbEntity } from '../constants/dbEntity'; // Adjust path if needed

@Entity(DbEntity.DailySequence)
export class DailySequenceEntity {
    // Stores the date in YYYYMMDD format as the primary key
    @PrimaryColumn({ type: 'varchar', length: 8 })
    date_key!: string;

    // The current sequence number for that date
    @Column({ type: 'int', default: 0 })
    sequence!: number;

    // Optional: To track when the sequence was last updated
    @UpdateDateColumn({ type: 'timestamp' })
    updated_at!: Date;
}