import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  BaseEntity,
} from 'typeorm';
import {
  Field,
  ObjectType,
  GraphQLISODateTime,
  ID,
  registerEnumType,
} from 'type-graphql';

import { PropertyTypeEnum } from './Property';

export enum ExtraFieldTypeEnum {
  STRING = 'string',
  NUMBER = 'number',
  BOOLEAN = 'boolean',
}

registerEnumType(ExtraFieldTypeEnum, {
  name: 'ExtraFieldTypeEnum',
  description: 'ExtraField types.',
});

@ObjectType()
@Entity('extraFields')
class ExtraField extends BaseEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Field()
  @Column()
  name: string;

  @Field(() => [ExtraFieldTypeEnum])
  @Column('enum', { name: 'type', array: true })
  type: ExtraFieldTypeEnum;

  @Field(() => [PropertyTypeEnum])
  @Column('enum', { name: 'properties_types', array: true })
  propertyTypes: PropertyTypeEnum[];

  @Field(() => GraphQLISODateTime)
  @CreateDateColumn()
  created_at: Date;

  @Field(() => GraphQLISODateTime)
  @UpdateDateColumn()
  updated_at: Date;
}

export default ExtraField;