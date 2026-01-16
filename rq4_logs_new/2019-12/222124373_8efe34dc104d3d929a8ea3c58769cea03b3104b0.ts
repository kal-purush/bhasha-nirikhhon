import { Entity, Column, PrimaryGeneratedColumn } from 'typeorm';
import { BaseEntity, Measure } from '@entity';

@Entity()
export class Measures extends BaseEntity<Measures> {
    @PrimaryGeneratedColumn()
    public id: number;

    @Column(() => Measure, { prefix: 'us_' })
    public us: Measure;

    @Column(() => Measure, { prefix: 'metric_' })
    public metric: Measure;

    public getDefault (): Measures {
        const measures = new Measures();
        measures.id = 0;
        measures.us = null;
        measures.metric = null;

        return measures;
    }
}