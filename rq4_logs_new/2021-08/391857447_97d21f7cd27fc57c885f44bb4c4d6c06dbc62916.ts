import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, PrimaryColumn } from "typeorm";
import { EPreferredRole, ESkills } from "../dataTypes/types";
import { Student } from "./student.entity";
import uuid from "uuid";
import { Group } from "./group.entity";
import { SharedCollection } from "./sharedCollection.entity";

@Entity({ name: "assignment" })
export class Assignment extends SharedCollection {
  @PrimaryGeneratedColumn("uuid")
  assignmentId: string;

  @Column({ nullable: true })
  numberOfGroups: Number;

  @Column("text", { array: true, nullable: true })
  groupsInThisAssignment: string[];

  @CreateDateColumn({ name: "created_at" })
  createdAt: Date;
}