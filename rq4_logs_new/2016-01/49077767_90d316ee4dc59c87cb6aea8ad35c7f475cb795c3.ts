import {Observable} from 'rxjs/Observable';

export module Meetup {

  export interface Provider {
    getList(city:string, country:string) : Observable<Model[]>;
  }

  export interface Parameters {
    get(param:string): string;
  }

  export class Model {
    constructor(public id:string,
                public name:string,
                public description:string,
                public urlName:string,
                public time:Date) {
    }

    static fromJSON(json:string) {
      return this.fromObject(JSON.parse(json));
    }

    static fromObject(obj) {
      return new Model(obj.id, obj.name, obj.description, obj.urlName, new Date(obj.time));
    }

    public toString() {
      return this.name;
    }
  }
}