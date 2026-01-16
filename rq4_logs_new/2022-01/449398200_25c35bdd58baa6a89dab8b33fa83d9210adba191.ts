import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";

@Entity("notebooks")
class Notebooks {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "varchar", nullable: true })
  imgName: string;

  @Column({ type: "varchar", nullable: true })
  imgPath: string;

  @Column("varchar")
  marca: string;

  @Column("varchar")
  modelo: string;

  @Column({ type: "varchar", nullable: true })
  numeroPatrimonio: string;

  @Column({ type: "varchar", nullable: true })
  numeroSerie: string;

  @CreateDateColumn({ type: "timestamp with time zone" })
  created_at: Date;

  @UpdateDateColumn({ type: "timestamp with time zone" })
  updated_at: Date;
}

export default Notebooks;