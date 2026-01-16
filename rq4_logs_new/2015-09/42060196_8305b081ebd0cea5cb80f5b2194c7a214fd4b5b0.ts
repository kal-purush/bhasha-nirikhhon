/**
 * Created by Daniel Beckwith on 9/6/15.
 */

///<reference path="_ref.d.ts"/>

module lsystems {

  export interface LRules {
    [index:string]:string;
  }

  var validChars:RegExp = /[A-Za-z\-+]/;

  export class LSystem {

    private _rules:LRules;

    constructor() {
      this._rules = {};
    }

    public get rules():LRules {
      return this._rules;
    }

    public addRule(symbol:string, replacement:string):void {
      if (symbol.length !== 1) {
        throw new LSymbolError('symbol can only be a single character');
      }
      _.forEach(replacement, (c:string, i:number):void => {
        if (!c.match(validChars)) {
          throw new LStringError(replacement, i, 'invalid character');
        }
      });
      this._rules[symbol] = replacement;
    }

    public step(lstring:string):string {
      var newString:string = '';
      _.forEach(lstring, (c:string, i:number):void => {
        var replacement:string = this._rules[c];
        newString += replacement || c;
      });
      return newString;
    }

  }

}