/*
  This is a copy of `no-void-expression` rule 
  extended with `ignore-patterns` option.
*/

import * as Lint from "tslint";
import { isTypeFlagSet } from "tsutils";
import ts from "typescript";

const OPTION_IGNORE_ARROW_FUNCTION_SHORTHAND =
  "ignore-arrow-function-shorthand";
const OPTION_IGNORE_PATTERNS = "ignore-patterns";

export class Rule extends Lint.Rules.TypedRule {
  public static metadata: Lint.IRuleMetadata = {
    ruleName: "no-void-expression-smart",
    description:
      "Requires expressions of type `void` to appear in statement position.",
    optionsDescription: Lint.Utils.dedent`
            If \`${OPTION_IGNORE_ARROW_FUNCTION_SHORTHAND}\` is provided, \`() => returnsVoid()\` will be allowed.
            Otherwise, it must be written as \`() => { returnsVoid(); }\`.
            You can provide \`${OPTION_IGNORE_PATTERNS}\` option as array of regex patterns to skip linting for
            particular code, f.e. \`{ "${OPTION_IGNORE_PATTERNS}": ["\(\) => skipMe\(\)"] }\``,
    options: {
      type: "list",
      listType: {
        anyOf: [
          {
            type: "string",
            enum: [OPTION_IGNORE_ARROW_FUNCTION_SHORTHAND]
          },
          {
            type: "object",
            properties: {
              [OPTION_IGNORE_PATTERNS]: {
                type: "array",
                items: { type: "string" },
                minLength: 1
              }
            },
            required: [OPTION_IGNORE_PATTERNS]
          }
        ]
      },
      minLength: 0,
      maxLength: 2
    },
    rationale: Lint.Utils.dedent`
            It's misleading returning the results of an expression whose type is \`void\`.
            Attempting to do so is likely a symptom of expecting a different return type from a function.
            For example, the following code will log \`undefined\` but looks like it logs a value:
            \`\`\`
            const performWork = (): void => {
                workFirst();
                workSecond();
            };
            console.log(performWork());
            \`\`\`
        `,
    requiresTypeInfo: true,
    type: "functionality",
    typescriptOnly: false
  };

  public static FAILURE_STRING =
    "Expression has type `void`. Put it on its own line as a statement.";

  public applyWithProgram(
    sourceFile: ts.SourceFile,
    program: ts.Program
  ): Lint.RuleFailure[] {
    const ignoreArrowFunctionShorthand =
      this.ruleArguments.indexOf(OPTION_IGNORE_ARROW_FUNCTION_SHORTHAND) !== -1;
    const { [OPTION_IGNORE_PATTERNS]: ignorePatterns = [] } =
      (this.ruleArguments.find(
        (arg: RawOptions) => arg && !!arg[OPTION_IGNORE_PATTERNS]
      ) as RawOptions) || {};
    return this.applyWithFunction(
      sourceFile,
      walk,
      { ignoreArrowFunctionShorthand, ignorePatterns },
      program.getTypeChecker()
    );
  }
}

interface RawOptions {
  [OPTION_IGNORE_ARROW_FUNCTION_SHORTHAND]?: string;
  [OPTION_IGNORE_PATTERNS]?: string[];
}

interface Options {
  ignoreArrowFunctionShorthand: boolean;
  ignorePatterns: string[];
}

function walk(ctx: Lint.WalkContext<Options>, checker: ts.TypeChecker): void {
  const {
    sourceFile,
    options: { ignoreArrowFunctionShorthand, ignorePatterns }
  } = ctx;
  ts.forEachChild(sourceFile, forEachChildWalker);

  function forEachChildWalker(node: ts.Node): void {
    if (
      isPossiblyVoidExpression(node) &&
      !isParentAllowedVoid(node) &&
      isTypeFlagSet(checker.getTypeAtLocation(node), ts.TypeFlags.Void) &&
      ignorePatterns.every(p => !new RegExp(p).test(node.getFullText()))
    ) {
      ctx.addFailureAtNode(node, Rule.FAILURE_STRING);
    }
    ts.forEachChild(node, forEachChildWalker);
  }

  function isParentAllowedVoid(node: ts.Node): boolean {
    switch (node.parent.kind) {
      case ts.SyntaxKind.ExpressionStatement:
        return true;
      case ts.SyntaxKind.ArrowFunction:
        return ignoreArrowFunctionShorthand;

      // Something like "x && console.log(x)".
      case ts.SyntaxKind.BinaryExpression:
        return isParentAllowedVoid(node.parent);

      // Something like "!!cond ? console.log(true) : console.log(false)"
      case ts.SyntaxKind.ConditionalExpression:
        return true;
      default:
        return false;
    }
  }
}

function isPossiblyVoidExpression(node: ts.Node): boolean {
  switch (node.kind) {
    case ts.SyntaxKind.AwaitExpression:
    case ts.SyntaxKind.CallExpression:
    case ts.SyntaxKind.TaggedTemplateExpression:
      return true;
    default:
      return false;
  }
}