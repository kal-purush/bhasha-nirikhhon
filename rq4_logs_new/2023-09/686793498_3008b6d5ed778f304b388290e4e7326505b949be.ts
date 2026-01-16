import {
  Column,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { Product } from "./Product";
import { ShoppingSession } from "./ShoppingSession";

@Index("FK_product_TO_cart_item", ["productId"], {})
@Index("FK_shopping_session_TO_cart_item", ["sessionId"], {})
@Entity("cart_item", { schema: "hivelink" })
export class CartItem {
  @PrimaryGeneratedColumn({ type: "int", name: "id" })
  id!: number;

  @Column("int", { name: "session_id" })
  sessionId!: number;

  @Column("int", { name: "product_id" })
  productId!: number;

  @Column("int", { name: "quantity" })
  quantity!: number;

  @Column("timestamp", { name: "created_at" })
  createdAt!: Date;

  @Column("timestamp", { name: "updated_at" })
  updatedAt!: Date;

  @ManyToOne(() => Product, (product) => product.cartItems, {
    onDelete: "NO ACTION",
    onUpdate: "NO ACTION",
  })
  @JoinColumn([{ name: "product_id", referencedColumnName: "id" }])
  product!: Product;

  @ManyToOne(
    () => ShoppingSession,
    (shoppingSession) => shoppingSession.cartItems,
    { onDelete: "NO ACTION", onUpdate: "NO ACTION" }
  )
  @JoinColumn([{ name: "session_id", referencedColumnName: "id" }])
  session!: ShoppingSession;
}