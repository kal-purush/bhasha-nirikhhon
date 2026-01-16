import { Table, Column, Model, DataType, PrimaryKey, Default, ForeignKey, BelongsTo } from 'sequelize-typescript'
import { User } from './User'
import { Doctor } from './Doctor'
import { TimeSlot } from './TimeSlot'
import { Service } from './Service'

@Table({ timestamps: true })
export class Booking extends Model {
  @PrimaryKey
  @Default(DataType.UUIDV4)
  @Column(DataType.UUID)
  id: string = ''

  @ForeignKey(() => User)
  @Column(DataType.UUID)
  patientId!: string

  @BelongsTo(() => User)
  patient!: User

  @ForeignKey(() => Doctor)
  @Column(DataType.UUID)
  doctorId!: string

  @BelongsTo(() => Doctor)
  doctor!: Doctor

  @Column(DataType.DATE)
  date!: Date

  @ForeignKey(() => TimeSlot)
  @Column(DataType.UUID)
  timeSlotId!: string

  @BelongsTo(() => TimeSlot)
  timeSlot!: TimeSlot

  @ForeignKey(() => Service)
  @Column(DataType.UUID)
  serviceId!: string

  @BelongsTo(() => Service)
  service!: Service

  @Column(DataType.STRING)
  status!: string
}