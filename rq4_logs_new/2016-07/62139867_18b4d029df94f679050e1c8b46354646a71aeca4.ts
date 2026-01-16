import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class V3Parser {

  private _parsedSchemas$: Subject<Object[]>
  private store: {
    schemas: Array<any>
  }

  public constructor() {
    this._parsedSchemas$ = <Subject<Object[]>> new Subject()
    this.store = {
      schemas : []
    }
  }

  get parsedSchemas$() {
    return this._parsedSchemas$.asObservable()
  }

  public parse(rawSchemas: string): void {
    this.store.schemas = JSON.parse(rawSchemas)
    console.log("Pass toooo Parser V3")
    this._parsedSchemas$.next(this.store.schemas)
  }

}