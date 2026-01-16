import {
    Entity,
    Column,
    PrimaryGeneratedColumn,
    CreateDateColumn,
    UpdateDateColumn,
    Index,
    ManyToOne,
    OneToMany,
    JoinColumn,
} from 'typeorm'
import { DbEntity } from '../constants/dbEntity'
import { UserEntity } from './User'
import { OrderTicketEntity } from './OrderTicket'
import { TicketEntity } from './Ticket'

/** 訂單狀態類型 */
export enum OrderStatus {
    /** 處理中 */
    PROCESSING = 'PROCESSING',
    /** 已完成 */
    COMPLETED = 'COMPLETED',
    /** 已取消 */
    CANCELLED = 'CANCELLED',
}

@Entity(DbEntity.Order)
@Index(['created_at'])
@Index(['order_number'])

/** 訂單 */
export class OrderEntity {
    /** 訂單id，主鍵 */
    @PrimaryGeneratedColumn()
    id!: number

    /** 用戶id，關聯user表，禁止為空 */
    @Column({ type: 'uuid', nullable: false })
    user_id!: string

    /** 場次id，關聯showtimes表，禁止為空 */
    @Column({ type: 'uuid', nullable: false })
    showtime_id!: string

    /** 訂單編號，唯一值，禁止為空 */
    @Column({ type: 'varchar', length: 20, unique: true, nullable: false })
    order_number!: string

    /** 訂單狀態，預設為PROCESSING，可選值：PROCESSING|COMPLETED|CANCELLED，禁止為空 */
    @Column({
        type: 'varchar',
        length: 20,
        nullable: false,
        default: 'PROCESSING',
        enum: OrderStatus,
    })
    status!: OrderStatus

    /** 購買張數，禁止為空 */
    @Column({ type: 'integer', nullable: false })
    total_count!: number

    /** 總金額，精度10位小數2位，禁止為空 */
    @Column({ type: 'numeric', precision: 10, scale: 2, nullable: false })
    total_price!: number

    /** 付款方式，預設為Credit Card，禁止為空 */
    @Column({
        type: 'varchar',
        length: 20,
        nullable: false,
        default: 'Credit Card',
    })
    payment_method!: string

    /** 付款狀態，預設為0，禁止為空 */
    @Column({ type: 'smallint', nullable: false, default: 0 })
    payment_status!: number

    /** 取票狀態，預設為0，禁止為空 */
    @Column({ type: 'smallint', nullable: false, default: 0 })
    pickup_status!: number

    /** 訂單建立時間，預設為當前時間，禁止為空 */
    @CreateDateColumn({
        type: 'timestamp',
        default: () => 'CURRENT_TIMESTAMP',
        nullable: false,
    })
    created_at!: Date

    /** 付款時間，可為空 */
    @Column({ type: 'timestamp', nullable: true })
    paid_at!: Date

    /** 資料更新時間，預設為當前時間，禁止為空 */
    @UpdateDateColumn({
        type: 'timestamp',
        default: () => 'CURRENT_TIMESTAMP',
        nullable: false,
    })
    updated_at!: Date

    /** 一個用戶有多個訂單 */
    @ManyToOne(() => UserEntity, (user) => user.orders)
    @JoinColumn({ name: 'user_id', referencedColumnName: 'id' })
    user!: UserEntity

    /** 一張訂單有多個區域票券記錄 */
    @OneToMany(() => OrderTicketEntity, (orderTicket) => orderTicket.order)
    orderTickets!: OrderTicketEntity[];

    /** 一張訂單有多張票券 */
    @OneToMany(() => TicketEntity, (ticket) => ticket.order)
    tickets!: TicketEntity[];
}