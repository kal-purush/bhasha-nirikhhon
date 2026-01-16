import { IInternshipEntity } from './internship';

export declare interface IBusinessEntity {
  id?: number;

  name: string;
  country: string;
  city: string;
  postalCode: string;
  address: string;
  additional?: string;

  internships?: IInternshipEntity[];

  createdAt?: Date;
  updatedAt?: Date;
}

/** @interface BusinessOpts Interface of all availables filters for businesses list */
export interface BusinessOpts {
  /** @property {string[]} countries List of selected countries to filter */
  countries?: string[];

  /** @property {string} name Part of name to apply filter on name field (database) */
  name?: string;
}