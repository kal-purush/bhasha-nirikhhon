/**
 * Created by Daniel Beckwith on 6/6/15.
 */

///<reference path="_ref.d.ts"/>

import lambda = require('./lambda');
import lambdacalc = require('./lambdacalc');
import _ = require('lodash');

function cli(args:string[]):void {
  console.log(_.repeat(lambdacalc.echo(args[2]), 4));

  console.log(new lambda.Variable(0).toString());
  var e:lambda.Function = new lambda.Function(0, new lambda.Function(1, new lambda.Application(new lambda.Variable(0),
    new lambda.Function(0, new lambda.Variable(0)))));
  console.log(e.toString());
  console.log(e.bindAll().toString());
}

export = cli;