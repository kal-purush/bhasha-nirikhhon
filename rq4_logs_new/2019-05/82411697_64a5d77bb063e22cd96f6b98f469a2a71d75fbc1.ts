import { PersistenceEngine } from "../PersistenceEngine";
import { Expressions, StrictExpressions, TypedExpression, IEnumerable } from "../expressions/expression";
import { ExpressionVisitor } from "../expressions/expression-visitor";
import { ExpressionType } from "../expressions/expression-type";
import { NewExpression } from "../expressions/new-expression";
import { ApplySymbolExpression } from "../expressions/apply-symbol-expression";
import { CallExpression } from "../expressions/call-expression";
import { MemberExpression } from "../expressions/member-expression";
import { TypedLambdaExpression, Parameter, LambdaExpression } from "../expressions/lambda-expression";
import { ConstantExpression } from "../expressions/constant-expression";
import { ParameterExpression } from "../expressions/parameter-expression";
import { UnaryExpression } from "../expressions/unary-expression";
import { BinaryExpression } from "../expressions/binary-expression";
import { QuerySymbols } from "../Query";
import { Exception, NotSupportedException } from "../exceptions";

export class Vanilla extends PersistenceEngine
{
    public load<T>(expression: StrictExpressions): PromiseLike<T[]>
    {
        return new Promise((resolve, reject) =>
        {
            var executor = new ExpressionExecutor();
            executor.result = store;
            executor.visit(expression);
            resolve(executor.result);
        })
    }
}

class ExpressionExecutor extends ExpressionVisitor
{
    constructor()
    {
        super();
    }

    result: any;

    visitUnknown(expression: { type: ExpressionType.Unknown, accept(visitor: ExpressionVisitor): any }): Expressions
    {
        if (expression.accept)
            return expression.accept(this as ExpressionVisitor);
        throw new Error("unsupported type");
    }

    visitNew<T>(expression: NewExpression<T>): Expressions
    {

        var result = {};

        for (var m of expression.init)
        {
            this.visit(m);
            result[m.member] = result;
        }

        this.result = result;
        return expression;
    }

    visitApplySymbol<T, U>(arg0: ApplySymbolExpression<T, U>): Expressions
    {
        let result;
        switch (arg0.symbol)
        {
            case QuerySymbols.any:
            case QuerySymbols.count:
                result = this.result;
                if (arg0.argument)
                {
                    result = [];
                    (this.result as any[]).filter((value, i) =>
                    {
                        this.result = value;
                        this.visit(arg0.argument);
                        if (this.result)
                            result.push(value);
                    });
                }

                this.result = result.length;
            case QuerySymbols.groupby:
                result = this.result;
                if (!arg0.argument)
                    throw new Exception('group by is missing the group criteria');

                var groups = {};
                (this.result as any[]).filter((value, i) =>
                {
                    this.result = value;
                    this.visit(arg0.argument);
                    if (typeof this.result == 'object')
                        throw new NotSupportedException('Not yet implemented');
                    if (typeof groups[this.result] == 'undefined')
                    {
                        groups[this.result] = [];
                        result.push({ key: this.result, value: groups[this.result] });
                    }
                    groups[this.result].push(value);

                });
                this.result = groups;
                break;
            case QuerySymbols.select:
                if (!arg0.argument)
                    throw new Exception('select is missing the select criteria');

                this.result = (this.result as any[]).map((value, i) =>
                {
                    this.result = value;
                    return this.visit(arg0.argument);
                });
                break;
            case QuerySymbols.where:
                result = [];
                if (!arg0.argument)
                    throw new Exception('select is missing the select criteria');

                this.result = (this.result as any[]).filter((value, i) =>
                {
                    this.result = value;
                    this.visit(arg0.argument);
                    return this.result;
                });

                break;
            case QuerySymbols.orderby:

                result = [];
                if (!arg0.argument)
                    throw new Exception('select is missing the select criteria');

                this.result = (this.result as any[]).sort((a, b) =>
                {
                    this.result = a;
                    this.visit(arg0.argument);
                    this.result = b;
                    this.visit(arg0.argument);
                    if (a < b)
                        return -1;
                    if (a == b)
                        return 0;
                    return 1;
                });
                break;
            case QuerySymbols.orderbyDesc:

                result = [];
                if (!arg0.argument)
                    throw new Exception('select is missing the select criteria');

                this.result = (this.result as any[]).sort((a, b) =>
                {
                    this.result = a;
                    this.visit(arg0.argument);
                    this.result = b;
                    this.visit(arg0.argument);
                    if (a < b)
                        return 1;
                    if (a == b)
                        return 0;
                    return -1;
                });
                break;
            case QuerySymbols.join: //lambda => binary(joincondition, otherSource)
                var lambda = arg0.argument as LambdaExpression;
                var binary = lambda.body as BinaryExpression<Expressions>;
                binary.left
                break;
        }

        return arg0;
    }

    visitCall<T, TMethod extends keyof T>(arg0: CallExpression<T, TMethod>): Expressions
    {
        var source = this.visit(arg0.source);
        var args = (this as ExpressionVisitor).visitArray(arg0.arguments) as StrictExpressions[];
        if (source !== arg0.source || args !== arg0.arguments)
        {
            if (!this.isTypedExpression(source))
                throw new Error('source is of type ' + source['type'] + ' and cannot be considered as a typed expression');
            return new CallExpression<T, TMethod>(source, arg0.method, args);
        }
        return arg0;
    }
    visitMember<T, TMember extends keyof T>(arg0: MemberExpression<T, TMember, T[TMember]>): Expressions
    {
        var source = this.visit(arg0.source);
        if (source !== arg0.source)
        {
            if (!this.isTypedExpression(source))
                throw new Error('source is of type ' + source['type'] + ' and cannot be considered as a typed expression');
            return new MemberExpression<T, TMember, T[TMember]>(source, arg0.member);

        }
        return arg0;
    }
    isTypedExpression<T>(source: Expressions): source is TypedExpression<T>
    {
        return source && (
            source.type == ExpressionType.ConstantExpression ||
            source.type == ExpressionType.ParameterExpression ||
            source.type == ExpressionType.MemberExpression ||
            source.type == ExpressionType.ApplySymbolExpression ||
            source.type == ExpressionType.NewExpression);
    }
    visitLambda<T extends (...args: any[]) => any>(arg0: TypedLambdaExpression<T>): Expressions
    {
        var parameters: Parameter<T> = this.visitArray(arg0.parameters) as any;
        var body = (this as ExpressionVisitor).visit(arg0.body);
        if (body !== arg0.body || parameters !== arg0.parameters)
            return new TypedLambdaExpression<T>(body, arg0.parameters);
        return arg0;
    }

    private static defaultComparer<T>(a: T, b: T)
    {
        return a === b;
    }

    visitConstant(arg0: ConstantExpression<any>): Expressions
    {
        return arg0;
    }
    visitParameter(arg0: ParameterExpression<any>): Expressions
    {
        return arg0;
    }
    visitUnary(arg0: UnaryExpression): Expressions
    {
        var operand = (this as ExpressionVisitor).visit(arg0.operand);
        if (operand !== arg0.operand)
            return new UnaryExpression(operand, arg0.operator);
        return arg0;
    }
    visitBinary<T extends Expressions = StrictExpressions>(expression: BinaryExpression<T>): BinaryExpression<Expressions>
    {
        var left = (this as ExpressionVisitor).visit(expression.left);
        var right = (this as ExpressionVisitor).visit(expression.right);

        if (left !== expression.left || right !== expression.right)
            return new BinaryExpression<Expressions>(left, expression.operator, right);
        return expression;
    }
}

var store: { [key: string]: any[] };