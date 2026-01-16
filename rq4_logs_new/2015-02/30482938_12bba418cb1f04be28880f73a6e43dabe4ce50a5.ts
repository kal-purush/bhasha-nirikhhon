
/// <reference path="typings/jquery/jquery.d.ts" />
/// <reference path="typings/x2js/xml2json.d.ts" />

module client {

export interface Result {
  value: number;
}

export var baseUri = 'http://localhost:8080/wadl2ts-example/rest/';
export var x2js = new X2JS();
export module calculator {
  export module add {
    export function get(arg1: number, arg2: number, callback: (response: Result) => void): void {
      $.ajax({dataType: 'xml', type: 'GET', url: baseUri + 'calculator/add', data: {arg1: arg1, arg2: arg2, }, success: (res: any)=>{callback(<Result>((<any>x2js.xml2json(res)).result));}});
    }
  }
}


}