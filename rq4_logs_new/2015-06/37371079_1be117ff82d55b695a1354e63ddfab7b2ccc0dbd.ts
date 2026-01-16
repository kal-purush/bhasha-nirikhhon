"use strict";
import parsimmon = require("parsimmon");
import Helper = require("./Helper");
import IndentContext = require("./IndentContext");
import IndentParser = require("./IndentParser");
import IndentResult = require("./IndentResult");
import Scenario = require("../scenario/Scenario");
import Monologue = require("../scenario/Monologue");
import Line = require("../scenario/Line");
import Novel = require("../scenario/Novel");
import Character = require("../scenario/Character");
import Position = require("../scenario/Position");
import Scene = require("../scenario/Scene");
import Choice = require("../scenario/Choice");
import Choices = require("../scenario/Choices");
import EndingParser = require("./EndingParser");
import ScenarioResult = require("./ScenarioResult");
import ScenarioParser = require("./ScenarioParser");

module NovelParser {

  function image(context: IndentContext, p: Position): parsimmon.Parser<Character> {
    return Helper.keyValueString("image").chain((image: string) =>
      IndentParser.newline.then(ScenarioParser.comment(context)).then(IndentParser.indent
        .chain((i: number) => {
          if (i === context.currentLevel) {
            return parsimmon.succeed(null);
          } else {
            return parsimmon.fail("indent don't equal");
          }
        })
        .then(Helper.keyValueArray("frames", Helper.int))
        .map((frames: number[]) => new Character(image, frames, p))
      ));
  }

  function stringToPos(p: string) {
    if (p === "left") {
      return Position.Left;
    } else if (p === "center") {
      return Position.Center;
    } else {
      return Position.Right;
    }
  }

  function character(position: string, context: IndentContext): parsimmon.Parser<IndentResult<Character>> {
    return ScenarioParser.block(
      context,
      position,
      (ctx: IndentContext) => image(ctx, stringToPos(position)).map((c: Character) => new IndentResult(c, ctx)));
  }

  // 手抜きのため順序固定
  // TODO: 重複を許さず3回まで出現を許容する
  export function characters(context: IndentContext): parsimmon.Parser<IndentResult<Character[]>> {
    var empty: IndentResult<Character[]> = new IndentResult([], context);
    return parsimmon.alt(character("left", context), character("center", context), character("right", context))
      .chain((res: IndentResult<Character>) =>
        parsimmon.lazy(() => characters(res.context)).map((result: IndentResult<Character[]>) => {
          let xs = result.value.slice();
          xs.unshift(res.value);
          return new IndentResult(xs, result.context);
        }))
      .or(parsimmon.succeed(empty));
  }

  function monologueBody(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.backgroundOption(context).chain((b: IndentResult<string>) =>
      characters(b.context).chain((cs: IndentResult<Character[]>) =>
        IndentParser.sameLevel(cs.context)
          .then(ScenarioParser.text(cs.context).map((ws: string[]) =>
            new IndentResult(new Monologue(ws, cs.value, b.value), cs.context))
      )));
  }

  export function monologue(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.block(context, "monologue", monologueBody);
  }

  var name: parsimmon.Parser<string> = Helper.keyValueString("name");

  function lineBody(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.backgroundOption(context).chain((b: IndentResult<string>) =>
      characters(b.context).chain((cs: IndentResult<Character[]>) =>
        name.chain((n: string) =>
          IndentParser.endOfLine(cs.context).chain((eolCtx: IndentContext) =>
            IndentParser.sameLevel(eolCtx)
              .then(ScenarioParser.text(eolCtx).map((ws: string[]) =>
                new IndentResult(new Line(n, ws, cs.value, b.value), eolCtx))
            )))));
  }

  export function line(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.block(context, "line", lineBody);
  }

  function choice(context: IndentContext): parsimmon.Parser<Choice[]> {
    let w = parsimmon.regex(new RegExp("[^:^\r^\n]*"));
    return IndentParser.sameLevel(context)
      .then(w.chain((c: string) =>
        Helper.lexeme(parsimmon.string(":")).then(Helper.word.map((s: string) => new Choice(c.trim(), s)))))
      .chain((ch: Choice) =>
        parsimmon.lazy(() => IndentParser.endOfLine(context).chain((eolCtx: IndentContext) => choice(eolCtx)))
          .map((res: Choice[]) => {
            let xs = res.slice();
            xs.unshift(ch);
            return xs;
          }).or(parsimmon.succeed([ch]))
        );
  }

  function choicesBody(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.backgroundOption(context).chain((b: IndentResult<string>) =>
      characters(b.context).chain((cs: IndentResult<Character[]>) =>
        choice(cs.context).map((ch: Choice[]) => new IndentResult(new Choices(b.value, cs.value, ch), cs.context))));
  }

  export function choices(context: IndentContext): parsimmon.Parser<IndentResult<Scene>> {
    return ScenarioParser.block(context, "choices", choicesBody);
  }

  export function scene(context: IndentContext): parsimmon.Parser<IndentResult<Scene[]>> {
    let empty: IndentResult<Scene[]> = new IndentResult([], context);
    return parsimmon.alt(monologue(context), line(context), choices(context))
      .chain((result: IndentResult<Scene>) =>
        parsimmon.lazy(() => scene(result.context)).map((res: IndentResult<Scene[]>) => {
          let es = res.value.slice();
          es.unshift(result.value);
          return new IndentResult(es, res.context);
        }))
      .or(parsimmon.succeed(empty));
  }

  type LoadScene = (read: (name: string) => string) => ScenarioResult<Scenario>;

  function nextScene(context: IndentContext): parsimmon.Parser<[string, LoadScene]> {
    return ScenarioParser.comment(context)
      .then(IndentParser.sameLevel(context))
      .then(Helper.keyValueString("next")
        .map<[string, LoadScene]>((name: string) =>
          [name, (read: (name: string) => string) => parse(read(name))]))
      .or(Helper.keyValueString("ending")
        .map((name: string) =>
          [name, (read: (name: string) => string) => EndingParser.parse(read(name))]));
  }

  export function novel(context: IndentContext): parsimmon.Parser<IndentResult<Novel>> {
    return ScenarioParser.scenarioInformation(context).chain((info: [string, string]) =>
      IndentParser.endOfLine(context).chain((eolCtx: IndentContext) =>
        scene(eolCtx).chain((result: IndentResult<Scene[]>) =>
          nextScene(result.context).map((nf: [string, LoadScene]) =>
            new IndentResult(new Novel(info[0], info[1], result.value, nf[0], nf[1]), result.context)))));
  }

  export function parse(input: string) {
    let context = IndentContext.initialize;
    return new ScenarioResult(novel(context).parse(input.trim()));
  }
}
export = NovelParser;