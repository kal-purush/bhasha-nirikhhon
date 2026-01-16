import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { tap, map } from 'rxjs/operators';

const cap = (s: string) => s.slice(0, 1).toLocaleUpperCase() + s.slice(1);
const words = (s: string) => {
  let str = '';
  for (let c of s) {
    str += (c > 'A' && c < 'Z' ? ` ${c}` : c);
  }
  return str;
};
const all = (schema: any, fn: (key: string, value: any) => string) =>
  Object.keys(schema || {})
    .map(key => fn(key, schema[key]))
    .filter(s => !!s)
    .join('\n');

const gen = (schema) => {
  const selector = schema.selector;
  const capSelector = cap(schema.selector);
  const stateClass = `${capSelector}State`;
  const initialName = `initial${stateClass}`;
  const typeEnum = `${capSelector}ActionType`;
  const defStrFn = (type, name, sch) => `
export ${type} ${name} {
${all(sch, (k, v) => type !== 'enum' ? `  ${k}: ${v};` : `  ${k} = ${v},`)}
}
  `;
  const actionStrFn = (type, name, payload) => `export class ${cap(name)}${capSelector} extends ${type}<${payload}> {
  type = ${typeEnum}.${name};
}`;
  const enumObjFn = (names) => names
    .map(name => [name, `'[${cap(words(selector))}] ${cap(words(name))}'`])
    .reduce((a, [k, v]) => ({ ...a, [k]: v }), {});

  return `

import { Injectable } from '@angular/core';
import { createSelector, select, Store } from '@ngrx/store';
import { Actions, Effect, ofType } from '@ngrx/effects';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';

${all(schema.defs, (name, sch) => defStrFn(sch.type, name, sch.value))}
${defStrFn('interface', stateClass, schema.state)}
export const ${initialName}: ${stateClass} = {
${all(schema.initial, (name, value) => `  ${name}: ${value},`)}
};

export const ${selector}StateSelector = '${selector}';
export const select${stateClass} = (states: any) => states[${selector}StateSelector] as ${stateClass};
${all(schema.state,
  (prop) => `export const select${capSelector}${cap(prop)} = createSelector(select${stateClass}, (state: ${stateClass}) => state.${prop});`
)}

${defStrFn('enum', typeEnum, enumObjFn(Object.keys(schema.actions)))}
${all(schema.actions, (name, sch) => actionStrFn(sch.type, name, sch.value))}

export type ${capSelector}Actions = ${Object.keys(schema.actions).map(name => `${cap(name)}${capSelector}`).join(('\n  | '))};

export function ${selector}StateReducer(state: ${stateClass} = ${initialName}, action: ${capSelector}Actions): ${stateClass} {
  switch (action.type) {
${all(schema.actions, (name, sch) => sch.type === 'RequestMessage' ? '' : `    case ${typeEnum}.${name}: {
      const p = (action as ${cap(name)}${capSelector}).payload;
      return {
        ...state,
      };
    }`)}
${all(schema.actions, (name, sch) => sch.type !== 'RequestMessage' ? '' : `    case ${typeEnum}.${name}:`)}
    default:
      return state;
  }
}

@Injectable()
export class ${capSelector}Facade {
  ${selector}$: Observable<${stateClass}> = this.store.pipe(select(select${stateClass}));
${all(schema.state, (name, value) => `  ${name}$: Observable<${value}> = this.store.pipe(select(select${capSelector}${cap(name)}));`)}
  constructor(
    private store: Store<any>,
  ) {}
${all(schema.actions, (name, sch) => sch.type === 'ResponseMessage' ? '' : `  ${name}(${sch.value === 'void' ? '' : `payload: ${sch.value}`}) {
    this.store.dispatch(new ${cap(name)}${capSelector}(${sch.value === 'void' ? '' : 'payload'}));
  }`)}
}

@Injectable()
export class ${capSelector}Effects {
  constructor(
    public actions$: Actions,
    public ${selector}Facade: ${capSelector}Facade,
  ) {}
${all(schema.actions, (name) => `  @Effect({ dispatch: false })
  ${name}$ = this.actions$.pipe(
    ofType(${typeEnum}.${name}),
    tap((${name}: ${cap(name)}${capSelector}) => console.log('${capSelector}Effects@${name}: ', ${name}))
  );`)}
}
`;

};


/*
  Generated class for the StoreGeneratorProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.
*/
@Injectable()
export class StoreGeneratorProvider {

  constructor(public http: HttpClient) {
    console.log('Hello StoreGeneratorProvider Provider');
  }

  generate(url: string): Observable<any> {
    return this.http.get(url).pipe(map((schema: any) => gen(schema)));
  }

}