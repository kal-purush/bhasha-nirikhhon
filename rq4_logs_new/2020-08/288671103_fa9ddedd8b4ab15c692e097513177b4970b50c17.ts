import { Collection } from "fireorm";

@Collection()
export default class ReportModel {
  id: string;
  date: Date;
}