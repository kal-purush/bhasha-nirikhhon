export interface FuseOptions {
  public id?: string;
  public keys?: any[];
  public caseSensitive?: boolean;
  public include?: string[];
  public maxPatternLength?: number;
  public shouldSort?: boolean;
  public distance?: number;
  public location?: number;
  public threshold?: number;
  public verbose?: boolean;
  public tokenize?: boolean;
  public searchFn(FuseSearchFn);
  public getFn(any, string): any;
  public sortFn(): any[];
};

export interface FuseSearchFn {
  constructor(string, FuseOptions);
  search(string): any[];
};


export class Fuse<T> {
  public list: T[];
  public options: FuseOptions;

  constructor(T[], FuseOptions);
  set(T[]): T[];
  search(string): T[];
};