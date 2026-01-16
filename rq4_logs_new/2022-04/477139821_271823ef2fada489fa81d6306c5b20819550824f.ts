import {
  Column,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  OneToOne,
} from "typeorm";
import { Transaction } from "./Transaction";
import { DetailMenu } from "./DetailMenu";

@Index("PK__DetailTr__A0DB881592AC5199", ["idTransaction"], { unique: true })
@Entity("DetailTransaction", { schema: "dbo" })
export class DetailTransaction {
  @Column("nvarchar", { primary: true, name: "idTransaction", length: 255 })
  idTransaction: string;

  @Column("int", { name: "qty", nullable: true })
  qty: number | null;

  @Column("float", { name: "intoMoney", nullable: true, precision: 53 })
  intoMoney: number | null;

  @OneToOne(() => Transaction, (transaction) => transaction.detailTransaction)
  @JoinColumn([
    { name: "idTransaction", referencedColumnName: "idTransaction" },
  ])
  idTransaction2: Transaction;

  @ManyToOne(() => DetailMenu, (detailMenu) => detailMenu.detailTransactions)
  @JoinColumn([
    { name: "idFood", referencedColumnName: "idFood" },
    { name: "idMenu", referencedColumnName: "idMenu" },
    { name: "idRestaurant", referencedColumnName: "idRestaurant" },
  ])
  detailMenu: DetailMenu;
}