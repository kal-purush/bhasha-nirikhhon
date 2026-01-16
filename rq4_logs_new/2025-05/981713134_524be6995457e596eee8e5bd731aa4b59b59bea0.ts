import { Column, Entity, PrimaryGeneratedColumn } from "typeorm";

@Entity("addresses", { schema: "ftn_db_main" })
export class Addresses {
  @PrimaryGeneratedColumn({ type: "int", name: "address_id", unsigned: true })
  addressId: number;

  @Column("varchar", { name: "first_name", nullable: true, length: 128 })
  firstName: string | null;

  @Column("varchar", { name: "last_name", nullable: true, length: 128 })
  lastName: string | null;

  @Column("varchar", { name: "country", nullable: true, length: 128 })
  country: string | null;

  @Column("varchar", { name: "city", nullable: true, length: 128 })
  city: string | null;

  @Column("varchar", { name: "address", nullable: true, length: 254 })
  address: string | null;

  @Column("varchar", { name: "postal_code", nullable: true, length: 32 })
  postalCode: string | null;
}