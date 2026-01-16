import { CouponCreatorType, CouponFilterType, CouponType } from 'src/enum';
import {
  Column,
  Entity,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  OneToMany,
} from 'typeorm';
import { Restaurant_Coupon } from './restaurant-coupon.entity';
import { SkusCoupon } from './skus-coupon.entity';
import { SysCategoryCoupon } from './sys-category-coupon.entity';

@Entity('Coupon')
export class Coupon {
  @PrimaryGeneratedColumn()
  public coupon_id: number;

  @Column({ type: 'varchar', length: 255, nullable: false, unique: false })
  public name: string;

  @Column({ type: 'text', nullable: true, unique: false })
  public description: string;

  @Column({
    type: 'enum',
    enum: CouponCreatorType,
    nullable: false,
    unique: false,
  })
  public creator_type: CouponCreatorType;

  @Column({ type: 'int', nullable: false, unique: false })
  public creator_id: number;

  @Column({ type: 'varchar', length: 10, nullable: false, unique: false })
  public coupon_code: string;

  @Column({ type: 'int', nullable: false, unique: false })
  public discount_value: number;

  @Column({ type: 'int', nullable: false, unique: false })
  public discount_unit: number;

  @Column({ type: 'tinyint', nullable: false, unique: false })
  public is_active: number;

  @Column({ type: 'bigint', nullable: false, unique: false })
  public valid_from: number;

  @Column({ type: 'bigint', nullable: false, unique: false })
  public valid_until: number;

  @Column({ type: 'int', nullable: true, unique: false })
  public budget: number;

  @Column({ type: 'int', nullable: true, unique: false })
  public mininum_order_value: number;

  @Column({ type: 'int', nullable: true, unique: false })
  public maximum_discount_amount: number;

  @Column({ type: 'tinyint', nullable: false, unique: false })
  public platform_sponsor_ratio_percentage: number;

  @Column({ type: 'bigint', nullable: true, unique: false })
  public out_of_budget: number;

  @Column({
    type: 'enum',
    enum: CouponType,
    nullable: false,
    unique: false,
  })
  public coupon_type: CouponType;

  @Column({
    type: 'enum',
    enum: CouponFilterType,
    nullable: false,
    unique: false,
  })
  public filter_type: CouponFilterType;

  @CreateDateColumn({
    type: 'datetime',
    nullable: false,
    unique: false,
    default: () => 'CURRENT_TIMESTAMP',
  })
  public created_at: Date;

  //Relationship

  @OneToMany(() => Restaurant_Coupon, (resCoup) => resCoup.coupon_obj)
  public restaurant_coupon_obj: Restaurant_Coupon[];

  @OneToMany(() => SkusCoupon, (coup) => coup.coupon_obj)
  public skus_coupon_obj: SkusCoupon[];

  @OneToMany(() => SysCategoryCoupon, (coup) => coup.coupon_obj)
  public sys_cat_coupon_obj: SysCategoryCoupon[];
}