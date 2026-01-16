import { CustomerType } from "../enums/customer-type.enum";
import { Pagination } from "./pagination.model";

interface Customer {
  code: number;
  type: CustomerType;
  document: string;
  fullName: string;
  cellphone: string;
  email: string;
  isActive: boolean;
}

export interface CustomerInputModel extends Customer {}

export interface CustomerOutputModel extends Customer {
  externalId: string;
}

export interface CustomerQueryParams extends Pagination {
  code?: number;
  types?: CustomerType[];
  document?: string;
  fullName?: string;
  email?: string;
  isActive?: boolean;
}