"use strict";
import parsimmon = require("parsimmon");
import Helper = require("./Helper");
import IndentContext = require("./IndentContext");
import IndentParser = require("./IndentParser");
import Result = require("./Result");
import Scenario = require("../scenario/Scenario");
import Monologue = require("../scenario/Monologue");
import Line = require("../scenario/Line");
import Scene = require("../scenario/Scene");
import Ending = require("../scenario/Ending");
import ScenarioParser = require("./ScenarioParser");

module EndingParser {

  export function scene(context: IndentContext): parsimmon.Parser<Result<Scene[]>> {
    let empty: Result<Scene[]> = new Result([], context);
    return parsimmon.alt(monologue(context), line(context))
      .chain((result: Result<Scene>) =>
        parsimmon.lazy(() => scene(result.context)).map((res: Result<Scene[]>) => {
          let es = res.value.slice();
          es.unshift(result.value);
          return new Result(es, res.context);
        }))
      .or(parsimmon.succeed(empty));
  }
  
  function monologueBody(context: IndentContext): parsimmon.Parser<Result<Scene>> {
    return IndentParser.sameLevel(context)
      .then(ScenarioParser.text(context).map((ws: string[]) => new Result(new Monologue(ws, [], null), context)));
  }

  export function monologue(context: IndentContext): parsimmon.Parser<Result<Scene>> {
    return ScenarioParser.block(context, "monologue", monologueBody);
  }

  var name: parsimmon.Parser<string> = Helper.keyValueString("name");

  function lineBody(context: IndentContext): parsimmon.Parser<Result<Scene>> {
    return IndentParser.sameLevel(context).then(name).chain((n: string) =>
      IndentParser.endOfLine(context).chain((eolCtx: IndentContext) =>
        IndentParser.sameLevel(eolCtx)
          .then(ScenarioParser.text(eolCtx).map((ws: string[]) => new Result(new Line(n, ws, [], null), eolCtx)))));
  }

  export function line(context: IndentContext): parsimmon.Parser<Result<Scene>> {
    return ScenarioParser.block(context, "line", lineBody);
  }

  export function novel(context: IndentContext): parsimmon.Parser<Result<Ending>> {
    return ScenarioParser.comment(context)
      .then(ScenarioParser.background(context)).chain((background: string) =>
        IndentParser.endOfLine(context)
        .chain((eolCtx: IndentContext) =>
          ScenarioParser.scene(eolCtx).map((result: Result<Scene[]>) =>
              new Result(new Ending(background, result.value), result.context)
            )
        ));
  }

  export function parse(input: string) {
    let context = IndentContext.initialize;
    return novel(context).parse(input.trim());
  }
}
export = EndingParser;