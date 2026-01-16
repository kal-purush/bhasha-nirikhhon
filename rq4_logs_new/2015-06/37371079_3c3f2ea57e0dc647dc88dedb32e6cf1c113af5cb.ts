"use strict";
import parsimmon = require("parsimmon");
import IndentContext = require("./IndentContext");
import IndentParser = require("./IndentParser");
import TreeNode = require("./TreeNode");

class TreeParser {

  static get elementName() {
    return parsimmon.letter.or(parsimmon.digit).many()
      .map((ns: string[]) => ns.join());
  }

  static elems(context: IndentContext): parsimmon.Parser<[TreeNode[], IndentContext]> {
    let empty: [TreeNode[], IndentContext] = [[], context];
    return TreeParser.element(context)
      .chain((tuple: [TreeNode, IndentContext]) =>
        TreeParser.elems(tuple[1]).map((res: [TreeNode[], IndentContext]) => {
          let es = res[0].slice();
          es.unshift(tuple[0]);
          let ret: [TreeNode[], IndentContext] = [es, res[1]];
          return ret;
        }))
      .or(parsimmon.succeed(empty));
  }

  static children(context: IndentContext): parsimmon.Parser<[TreeNode[], IndentContext]> {
    var currentLevel = context.currentLevel;
    return IndentParser.openParen(currentLevel, context).chain((openCtx: IndentContext) =>
      parsimmon.lazy(() => TreeParser.elems(openCtx))
        .chain((result: [TreeNode[], IndentContext]) => {
          var lines = result[0];
          return IndentParser.closeParen(currentLevel, result[1]) .map((closeCtx: IndentContext) => {
            let ret: [TreeNode[], IndentContext] = [lines, closeCtx];
            return ret;
          });
        }));
  }

  static opt<T>(parser: parsimmon.Parser<T>): parsimmon.Parser<T> {
    return parser.or(parsimmon.succeed(null));
  }

  static element(context: IndentContext): parsimmon.Parser<[TreeNode, IndentContext]> {
    return IndentParser.sameLevel(context).then(
      TreeParser.elementName.chain((n: string) =>
        IndentParser.endOfLine(context).chain((eolCtx: IndentContext) =>
          parsimmon.lazy(() =>
            TreeParser.opt(TreeParser.children(eolCtx)).map((tuple: [TreeNode[], IndentContext]) => {
              if (tuple) {
                let r1: [TreeNode, IndentContext] = [new TreeNode(n, tuple[0]), tuple[1]];
                return r1;
              } else {
                let r2: [TreeNode, IndentContext] = [new TreeNode(n), eolCtx];
                return r2;
              }
            })))));
  }
}
export = TreeParser;