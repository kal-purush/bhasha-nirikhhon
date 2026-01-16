import * as moment from "moment";
import { v4 as uuid } from "uuid";
import { IEventsDataSource } from "./events-data-source";
import { IEvent } from "../../common/models/event";

const events: IEvent[] = [
  {
    id: "c64b1314-64ab-4fcf-99a1-df9edd1307ce",
    name: "Tortas para la oficina 1",
    startDate: 1548000000, // epoch
    expirationDate: 1549000000, // epoch
    startHour: 800,
    endHour: 1200,
    createdBy: "90ec45da-452b-4c37-a5fc-482c8bc92895",
    total: 22,
    cancelled: false,
    markedAsFinished: true,
    createdAt: 1548000000,
    updatedAt: 1548000000,
    orders: []
  },
  {
    id: "8022f792-40cf-43ef-b72d-ba42de2117d3",
    name: "Tortas para la oficina 2",
    startDate: 1548500000, // epoch
    expirationDate: 1549500000, // epoch
    startHour: 800,
    endHour: 1200,
    createdBy: "a79639e6-3ed9-467c-b9c5-1e60908d812c",
    total: 10,
    cancelled: true,
    markedAsFinished: true,
    createdAt: 1548000000,
    updatedAt: 1548000000,
    orders: []
  },
  {
    id: "8c9ae830-dd56-4828-8503-c70355253de9",
    name: "Tortas para la oficina 3",
    startDate: 1548500000, // epoch
    expirationDate: 1549500000, // epoch
    startHour: 800,
    endHour: 1200,
    createdBy: "6d623d08-113c-4565-81b2-e17c90331241",
    total: 20,
    cancelled: false,
    markedAsFinished: false,
    createdAt: 1548000000,
    updatedAt: 1548000000,
    orders: []
  }
];

const find = (query?: any): IEvent[] => {
  if (query) {
    const [key]: string[] = Object.keys(query);
    return events.filter((order: IEvent) => order[key] === query[key]);
  }
  return events;
};

const save = (event: IEvent): IEvent => {
  event.id = uuid();
  const result: IEvent = {
    ...event,
    total: 0,
    markedAsFinished: false,
    cancelled: false,
    createdAt: moment()
      .utc()
      .unix(),
    updatedAt: moment()
      .utc()
      .unix()
  };
  events.push(result);
  return result;
};

const update = (event: IEvent): IEvent => {
  const index: number = events.findIndex(
    (even: IEvent) => even.id === event.id
  );
  events[index] = { ...events[index], ...event };
  return events[index];
};

export const eventsMemoryDataSource: IEventsDataSource = { find, save, update };