import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from "typeorm";
import { DeviceEntity } from "./DeviceEntity";

@Entity("device_category", { schema: "hivelink" })
export class DeviceCategoryEntity {
  @PrimaryGeneratedColumn({ type: "int", name: "id" })
  id!: number;

  @Column("varchar", { name: "name", length: 50 })
  name!: string;

  @Column("text", { name: "description", nullable: true })
  description!: string | null;

  @Column("timestamp", { name: "created_at" })
  createdAt!: Date;

  @Column("timestamp", { name: "updated_at" })
  updatedAt!: Date;

  @OneToMany(() => DeviceEntity, (device) => device.category)
  devices!: DeviceEntity[];
}