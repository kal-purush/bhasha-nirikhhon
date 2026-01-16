import { IEvent, IEventDto } from "../models/event";
import * as orderMapper from "../mappers/order";

export const toDto = (event: IEvent): IEventDto => ({
  id: event.id,
  name: event.name,
  start_date: event.startDate,
  expiration_date: event.expirationDate,
  start_hour: event.startHour,
  end_hour: event.endHour,
  created_by: event.createdBy,
  total: event.total,
  orders: event.orders.map(orderMapper.toDto),
  marked_as_finished: event.markedAsFinished,
  cancelled: event.cancelled,
  created_at: event.createdAt,
  updated_at: event.updatedAt
});

export const toModel = (event: IEventDto): IEvent => ({
  id: event.id,
  name: event.name,
  startDate: event.start_date,
  expirationDate: event.expiration_date,
  startHour: event.start_hour,
  endHour: event.end_hour,
  createdBy: event.created_by,
  total: event.total,
  orders: event.orders.map(orderMapper.toModel),
  markedAsFinished: event.marked_as_finished,
  cancelled: event.cancelled,
  createdAt: event.created_at,
  updatedAt: event.updated_at
});