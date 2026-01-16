import { Field, Int, ObjectType } from "type-graphql";
import { BaseEntity, Column, Entity, OneToMany, PrimaryColumn } from "typeorm";
import { SubIndustry } from "./SubIndustry";

@ObjectType()
@Entity()
export class Industry extends BaseEntity {
  @Field(() => Int)
  @PrimaryColumn()
  id: number;

  @Field()
  @Column()
  code: string;

  @Field()
  @Column()
  nameFi: string;

  @Field()
  @Column()
  nameEn: string;

  @Field(() => [SubIndustry])
  @OneToMany(() => SubIndustry, (s) => s.industry)
  subIndustries: SubIndustry[];
}