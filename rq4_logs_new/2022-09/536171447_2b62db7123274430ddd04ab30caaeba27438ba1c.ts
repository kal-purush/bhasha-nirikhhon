import { Plank } from 'src/planks/entity/plank.entity';
import { User } from 'src/users/entity/user.entity';
import {
  BaseEntity,
  Column,
  Entity,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
} from 'typeorm';
import { Bool } from 'types';

@Entity()
export class Day extends BaseEntity {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({
    width: 5,
    type: 'smallint',
    unsigned: true,
  })
  numeration: number;

  @Column({
    type: 'decimal',
    precision: 32,
    scale: 29,
    unsigned: true,
  })
  weight: number;

  @Column({
    width: 5,
    type: 'smallint',
    unsigned: true,
  })
  caloriesBurnt: number;

  @Column({
    width: 8,
    type: 'mediumint',
    unsigned: true,
  })
  bestPlankTime: number;

  @ManyToOne(() => User, (user) => user.days, {
    cascade: true,
  })
  user: User;

  @Column({
    type: 'enum',
    enum: Bool,
  })
  isFinished: Bool;

  @Column({
    type: 'datetime',
  })
  dateOfBirth: Date;

  @OneToMany(() => Plank, (plank) => plank.day)
  planks: Plank[];
}