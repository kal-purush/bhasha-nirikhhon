declare module 'filter-object' {

  interface FilterOptions {
    keys?: string[];
    sort?: (key1: string, key2: string) => number;
    sortOrder?: 'asc' | 'desc' | 'ASC' | 'DESC' | string;
    sortBy?: (obj: any) => string[];
  }
  interface FilterObject {
    (obj: any, globs: string | string[], opts?: FilterOptions): any;
    (obj: any, filter: Function, opts?: FilterOptions): any;
  }

  const filterObject: FilterObject;

  export = filterObject;
}