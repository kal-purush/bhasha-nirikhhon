export enum TechnologyType {
  Blue,
  Red,
  Yellow,
  Green
}

export interface ITechnology {
  id: number;
  name: string;
  type: TechnologyType;
  prerequisite: number[];
  status: string;
  researched: boolean;
  unitAdjustment: string;
}

export class Technology {
  id: number;
  name: string;
  type: TechnologyType;
  prerequisite: number[];
  researched: boolean;
  unitAdjustment: string;

  constructor (params: ITechnology) {
    this.id = params.id
    this.name = params.name;
    this.type =  params.type;
    this.prerequisite = params.prerequisite;
    this.researched = params.researched;
    this.unitAdjustment = params.unitAdjustment;
  }
}