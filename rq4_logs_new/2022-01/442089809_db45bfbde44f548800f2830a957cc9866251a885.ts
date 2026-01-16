import { Column, PrimaryGeneratedColumn, Entity } from "typeorm";
import "express-async-errors";
import { ValidationEntity } from "./validation/validation.js";
import { IsEmail } from "class-validator";

export interface authEntity {
  id: number;
  email: string;
  username: string;
  password: string;
  position: "seller" | "buyer";
}

@Entity()
export class User extends ValidationEntity implements authEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @IsEmail()
  @Column()
  email: string;

  @Column()
  username: string;

  @Column()
  password: string;

  @Column()
  position: "seller" | "buyer";
}