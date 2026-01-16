/// <reference path="../typings/jquery/jquery.d.ts" />
module Murataku {
    export class Person {
        age:number;
        name:string;
    }
}

var ore:Murataku.Person = {
        age: 26,
        name: "Takuya Murakami"
    };

$("div.hoge").text("fuga")