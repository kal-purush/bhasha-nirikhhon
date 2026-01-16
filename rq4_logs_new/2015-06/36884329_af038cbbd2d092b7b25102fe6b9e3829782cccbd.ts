///<reference path="./lib/jquery.d.ts" />

import model = require("./model");
import jquery = require("jquery");

function point(xn: number, yn: number) {
    var x = new model.NumericExpression();
    x._value = xn;
    var y = new model.NumericExpression();
    y._value = yn;
    var p = new model.Point();
    p._args.x = x;
    p._args.y = y;
    return p;
}

declare var window: any;

function start(data: string): void {
    var program = new model.Reader(<HTMLCanvasElement>document.getElementById("canvas")).read(JSON.parse(data));
    //var program = new model.Program(<HTMLCanvasElement>document.getElementById("canvas"));
    //
    //program._rootStatement.add(point(5, 5));
    //program._rootStatement.add(point(15, 15));

    while (program._rootStatement.execute()) {
        console.log(".");
    };

    window.program = program;
}

jquery.ajax(
    {
        url: "data/dots.json",
        type: "GET",
        dataType: "text",
        success: start
    }).done(start);