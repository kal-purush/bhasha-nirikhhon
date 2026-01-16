import { isString } from "node:util";
import { Entity, PrimaryGeneratedColumn, Column } from "typeorm";
import { EPreferredRole, ESkills } from "../dataTypes/types";

// The property "name" sets the table name. This is usually implied from the
// class name, however this can be overridden if needed.
@Entity()
export class Student {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column()
  firstName: string;

  @Column()
  lastName: string;

  @Column("text", { array: true })
  preferredRole: EPreferredRole[];

  @Column("text", { array: true })
  skills: ESkills[];

  @Column({ name: "created_at" })
  createdAt: Date;
}