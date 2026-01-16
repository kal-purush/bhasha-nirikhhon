import * as Promise from 'bluebird';

import Orchestrator from "./orchestrator";
import { DataSet } from "./model";

function Orchestration<T, U>(orchestrator: Orchestrator<T, U>) : ((initialData : { [K in keyof T]?: T[K] }) => DataSet<T>) {

  function Constructor(initialData: { [K in keyof T]?: T[K] }) {
    this.cache = Object.assign({}, initialData || {});
  }

  Constructor.prototype = orchestrator.mapSpec(key => function(cb) {
    return orchestrator.resolve(key, this.cache, cb);
  });

  Object.assign(Constructor.prototype, {
    get(...args) {
      if (typeof(args[args.length - 1]) === 'function') {
        return Promise.resolve(orchestrator.fetchDependentResult(this.cache, null, args.slice(0, -1), results => results))
          .asCallback(args[args.length - 1]);
      } else {
        return orchestrator.fetchDependentResult(this.cache, null, args, results => results);
      }
    },
    toJSON() {
      return this.cache;
    }
  });

  return data => new Constructor(data);
}

export default Orchestration;