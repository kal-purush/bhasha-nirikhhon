import { Field, ID, ObjectType } from "type-graphql";
import {
  BaseEntity,
  Column,
  Entity,
  JoinTable,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { BusinessField } from "./BusinessField";
import { Municipality } from "./Municipality";

@ObjectType()
@Entity()
export class RegistrationRequest extends BaseEntity {
  @Field(() => ID)
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Field()
  @Column()
  lastName: string;

  @Field()
  @Column()
  firstName: string;

  @Field()
  @Column()
  email: string;

  @Field()
  @Column()
  businessID: string;

  @Field(() => Municipality)
  @ManyToOne(() => Municipality)
  municipality: Municipality;

  @Field(() => BusinessField)
  @ManyToOne(
    () => BusinessField,
    (businessField) => businessField.organizations
  )
  @JoinTable()
  businessField: BusinessField;
}