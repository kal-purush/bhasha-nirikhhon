import { Pagination } from "./pagination.model";
import { UnitOfMeasurementOutputModel } from "./unit-of-measurement.model";

interface Product {
  code: number;
  name: string;
  description?: string;
  costPrice?: number;
  profitMargin?: number;
  sellingPrice: number;
  stock: number;
  isActive: boolean;
}

export interface ProductInputModel extends Product {
  unitOfMeasurement: string;
}

export interface ProductOutputModel extends Product {
  externalId: string;
  unitOfMeasurement: UnitOfMeasurementOutputModel;
}

export interface ProductQueryParams extends Pagination {
  code?: number;
  name?: string;
  sellingPrice?: number;
  stock?: number;
  unitsOfMeasurement?: string[];
  isActive?: boolean;
}