import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";

@Entity("celulares")
export class Celular {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "string", nullable: true })
  imgName: string;

  @Column({ type: "string", nullable: true })
  imgPath: string;

  @Column("string")
  nome: string;

  @Column("string")
  modelo: string;

  @Column("string")
  imeis: string[];

  @CreateDateColumn({ type: "timestamp with time zone" })
  created_at: Date;

  @UpdateDateColumn({ type: "timestamp with time zone" })
  updated_at: Date;
}