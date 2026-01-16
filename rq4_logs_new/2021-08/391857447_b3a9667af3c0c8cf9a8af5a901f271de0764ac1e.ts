import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn } from "typeorm";
import { EPreferredRole, ESkills } from "../dataTypes/types";
import { Student } from "./student.entity";
import uuid from "uuid";

@Entity({ name: "group" })
export class Group {
  @PrimaryGeneratedColumn("uuid")
  groupId: string;

  @Column("enum", { array: true, nullable: true, enum: EPreferredRole, default: [] })
  rolesRequired: EPreferredRole[];

  @Column("text", { array: true, nullable: true, default: [] })
  studentsInGroup: string[]; // This will store the uuid of students in the group

  @Column("enum", { array: true, nullable: true, enum: ESkills, default: [] })
  skillsRequired: ESkills[];

  @Column({ nullable: true })
  maxSizeOfGroup: Number;

  @CreateDateColumn({ name: "created_at" })
  createdAt: Date;
}