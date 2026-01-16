import { Injectable } from '@angular/core';

export class Procedure {
    type: string;               // field
    name: string;               // procedure name
    survivalRate?: number;
}

const procedureList: Procedure[] = [
    {
        type: 'Endodontics',
        name: 'Root Canal Treatment',
        survivalRate: 3
    },
    {
        type: 'Endodontics',
        name: 'Retreatment',
        survivalRate: 3
    },
    {
        type: 'Prosthodontics',
        name: 'Onlay',
        survivalRate: 3
    }
];

@Injectable({providedIn: 'root'})
export class ProcedureService {

  constructor() { }

  getProcedures() {
    return procedureList;
  }
}