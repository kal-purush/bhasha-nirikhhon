import { Entity, Column, PrimaryGeneratedColumn } from 'typeorm';
import { BaseEntity } from '@entity';

@Entity()
export class InstructionStep extends BaseEntity<InstructionStep> {
    @PrimaryGeneratedColumn()
    public id: number;

    @Column()
    public number: number;

    @Column()
    public step: string;

    'ingredients': [];

    'equipment': [];

    'length': {};

    getDefault (): InstructionStep {
        const instruction = new InstructionStep();
        instruction.id = 0;
        instruction.number = 0;
        instruction.step = '';
        instruction.ingredients = null;
        instruction.equipment = null;
        instruction.length = null;

        return instruction;
    }
}