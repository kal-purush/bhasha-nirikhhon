/*---------------------------------------------------------
 * Copyright 2016 otpl-node Author. All rights reserved.
 *--------------------------------------------------------*/

import * as opc from './opcodes';

/**
* Node
*/
export class Node {
    constructor(public line: number, public column: number) {

    }

    compile(buf: opc.Opcode[], start?: opc.Opcode, end?: opc.Opcode) {

    }
}

/**
 * NodeList
 */
export class NodeList extends Node {
    childen: Node[]
    constructor(items?: Node[]) {
        super(0, 0);
        this.childen = items || [];
    }

    get count() {
        return this.childen.length;
    }

    append(node: Node) {
        this.childen.push(node);
    }

    compile(buf: opc.Opcode[], start?: opc.Opcode, end?: opc.Opcode) {
        for (let node of this.childen) {
            node.compile(buf, start, end);
        }
    }

}

class Primitive extends Node {
    constructor(public value: any, public datatype: opc.DataType) {
        super(0, 0);
    }

    compile(buf: opc.Opcode[]) {
        let op = new opc.LoadConst(this.line, this.column);
        op.value = this.value;
        op.datatype = this.datatype;
        buf.push(op);
    }
}

export class Float extends Primitive {
    constructor(value: string) {
        super(value, opc.DataType.Float);
    }
}

export class Integer extends Primitive {
    constructor(value: string) {
        super(value, opc.DataType.Integer);
    }
}

export class String extends Primitive {
    constructor(value: string) {
        super(value, opc.DataType.String);
    }
}

export class Boolean extends Primitive {
    constructor(value: any) {
        value = ((value || '') + '').toLowerCase();
        super(value, value === 'true' ? opc.DataType.True : opc.DataType.False);
    }
}

export class None extends Primitive {
    constructor() {
        super('null', opc.DataType.Null);
    }
}

/**
 * 表示一个模板数据
 */
export class Data extends Node {
    constructor(line: number, column: number, public value: string) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        let op = new opc.LoadConst(this.line, this.column);
        op.value = this.value;
        op.datatype = opc.DataType.String;
        buf.push(op);
    }
}

/**
 * 表示一个标识符
 */
export class Identifier extends Node {
    constructor(line: number, column: number, public value: string) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        let op = new opc.LoadVariable(this.line, this.column);
        op.name = this.value;
        buf.push(op);
    }
}



/**
 * 表示模板根节点
 */
export class Root extends NodeList {
    private blocks = new NodeList()
    private layout: Layout
    constructor(private file: string, private mtime: number) {
        super()
    }

    append(node: Node) {
        if (node instanceof Block) {
            this.blocks.append(node);
        }
        else if (node instanceof Layout) {
            if (this.layout) {
                throw "layout 只能出现一次";
            }
            this.layout = node;
        }
        else {
            super.append(node);
        }
    }

    compile(buf: opc.Opcode[]) {
        let op = new opc.Document(this.line, this.column);
        op.endHeader = new opc.Nop(this.line, this.column);
        op.filename = this.file;
        op.mtime = this.mtime;

        buf.push(op);
        //write blocks
        this.blocks.compile(buf);
        //write header end
        buf.push(op.endHeader);
        //write layout
        if (this.layout) {
            this.layout.compile(buf);
        }
        //write body
        super.compile(buf);
        //write exit
        buf.push(new opc.Exit());
    }
}
/**
 * 表示该节点将被忽略
 */
export class Skip extends Node {
    constructor(public isBreak: boolean) {
        super(0, 0);
    }
}

/**
 * Filter
 */
export class Filter extends Node {
    constructor(line: number, column: number, public name: string, public parameters: NodeList) {
        super(line, column);
    }

    compile(buf: opc.Opcode[]) {
        this.parameters.compile(buf);
        (new String(this.name)).compile(buf);
        var op = new opc.Call(this.line, this.column);
        op.parameters = this.parameters.count + 1; //加上要过滤的对象作为第一个参数
        buf.push(op);
    }
}

/**
 * Print
 */
export class Print extends Node {
    constructor(line: number, column: number, public primary: Node, public escape: boolean, public filters: Filter[]) {
        super(line, column);
    }

    compile(buf: opc.Opcode[]) {
        this.primary.compile(buf);
        if (this.filters && this.filters.length > 0) {
            for (let filter of this.filters) {
                filter.compile(buf);
            }
        }
        let op = new opc.Print(this.line, this.column);
        op.escape = this.escape;
        buf.push(op);
    }

}

/**
 * 表示包含二元运算符的表达式。
 */
export class Binary extends Node {
    constructor(line: number, column: number, public left: Node, public right: Node, public operator: string) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        switch (this.operator) {
            case '==': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Eq;
                buf.push(op);
                break;
            }
            case '!=': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Ne;
                buf.push(op);
                break;
            }
            case '<': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Lt;
                buf.push(op);
                break;
            }
            case '<=': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Le;
                buf.push(op);
                break;
            }
            case '>': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Gt;
                buf.push(op);
                break;
            }
            case '>=': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Ge;
                buf.push(op);
                break;
            }

            case '+': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Add;
                buf.push(op);
                break;
            }
            case '-': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Sub;
                buf.push(op);
                break;
            }
            case '*': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Mul;
                buf.push(op);
                break;
            }
            case '/': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Div;
                buf.push(op);
                break;
            }
            case '%': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Mod;
                buf.push(op);
                break;
            }
            case '&&': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.And;
                buf.push(op);
                break;
            }
            case '||': {
                this.left.compile(buf);
                this.right.compile(buf);
                var op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Or;
                buf.push(op);
                break;
            }

            case '??': {

                let start = new opc.Nop(this.line, this.column);
                let end = new opc.Nop(this.line, this.column);
                //取得表达式1的值并设置临时变量
                this.left.compile(buf);
                let sv = new opc.SetVariable(this.line, this.column);
                sv.name = opc.randomName();
                buf.push(sv);
                //如表达式1 不为 null 则结束
                let lv = new opc.LoadVariable(this.line, this.column);
                lv.name = sv.name;
                buf.push(lv);
                let ld = new opc.LoadConst(this.line, this.column);
                ld.datatype = opc.DataType.Null;
                buf.push(ld);
                let op = new opc.Operation(this.line, this.column);
                op.operator = opc.Operator.Eq;
                buf.push(op);
                let go = new opc.Goto(this.line, this.column);
                go.flag = opc.Goto.FALSE;
                go.target = start;
                buf.push(go);
                //取得表达式2的值
                this.right.compile(buf);
                go = new opc.Goto(this.line, this.column);
                go.flag = opc.Goto.NEVER;
                go.target = end;
                buf.push(go);

                //获取临时值
                buf.push(start);
                lv = new opc.LoadVariable(this.line, this.column);
                lv.name = sv.name;
                buf.push(lv);
                //结束
                buf.push(end);

                break;
            }

            default:
                throw "运算符未定义：" + this.operator;
        }
    }
}

/**
 * Ternary
 */
export class Ternary extends Node {
    constructor(line: number, column: number, public condition: Node, public left: Node, public right: Node) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        let start = new opc.Nop(this.line, this.column);
        let end = new opc.Nop(this.line, this.column);
        //计算条件
        this.condition.compile(buf);
        let go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.FALSE;
        go.target = start;
        buf.push(go);
        //计算左值
        this.left.compile(buf);
        go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.NEVER;
        go.target = end;
        buf.push(go);
        //计算右值
        buf.push(start);
        this.right.compile(buf);
        //结束
        buf.push(end);
    }
}

/**
 * 表示包含一元运算符的表达式。
 */
export class Unary extends Node {
    constructor(line: number, column: number, public body: Node, public operator: string) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        let operator: opc.Operator;
        switch (this.operator) {
            case 'neg':
                operator = opc.Operator.Neg;
                break;
            case 'pos':
                operator = opc.Operator.Pos;
                break;
            default:
                throw new Error('未定义的操作符: ' + this.operator);
        }
        this.body.compile(buf);
        let op = new opc.Operation(this.line, this.column);
        op.operator = operator;
        buf.push(op);

    }
}


export class Property extends Node {
    getMethod: boolean
    constructor(line: number, column: number, public body: Node, public parameters: NodeList) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        this.parameters.compile(buf);
        this.body.compile(buf);
        var lm = new opc.LoadMember(this.line, this.column);
        lm.parameters = this.parameters.count;
        buf.push(lm);
    }
}

export class Method extends Node {
    constructor(line: number, column: number, public body: Node, public parameters: NodeList) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {
        // if(this.name==='range'){
        // 	var op=new il.RangeIterator()
        // }
        // var op = new il.GetLocal(this.line);
        // op.name = this.id;
        // buf.push(op);
        //console.log(this.obj)
        this.parameters.compile(buf);
        if (this.body instanceof Property) {
            (<Property>this.body).getMethod = true;
            this.body.compile(buf);
        }
        else if (this.body instanceof Symbol) {
            (new String((<Identifier>this.body).value)).compile(buf);//直接写入函数名称
        }

        var op = new opc.Call(this.line, this.column);
        op.parameters = this.parameters.count;
        buf.push(op);
    }
}

export class Block extends NodeList {
    constructor(line: number, column: number, public id: string) {
        super();
    }

    compile(buf: opc.Opcode[]) {
        var op = new opc.Block(this.line, this.column);
        op.id = this.id;
        buf.push(op);
        super.compile(buf);
        buf.push(new opc.Exit(this.line, this.column));
    }
}

export class BlockCall extends Node {
    constructor(line: number, column: number, public id: string, public parameters: NodeList) {
        super(line, column);
    }
    compile(buf: opc.Opcode[]) {

        if (this.id !== 'body') {
            let sc = new opc.Scope(this.line, this.column);//创建新的作用域
            sc.restore = false;
            buf.push(sc);
        }


        //参数
        this.parameters.compile(buf);

        //调用
        let bc = new opc.BlockCall(this.line, this.column);
        bc.id = this.id;
        bc.parameters = this.parameters.count;
        buf.push(bc);


        if (this.id !== 'body') {
            let sc = new opc.Scope(this.line, this.column);//创建新的作用域
            sc.restore = true;
            buf.push(sc);
        }

    }
}

export class Set extends Node {
    constructor(line: number, column: number, public name: string, public value: Node) {
        super(line, column);
    }

    compile(buf: opc.Opcode[]) {
        this.value.compile(buf);
        let op = new opc.SetVariable(this.line, this.column);
        op.name = this.name;
        buf.push(op);
    }
}

export class Layout extends Node {
    constructor(line: number, column: number, public src: string) {
        super(line, column);
    }

    compile(buf: opc.Opcode[]) {
        var op = new opc.Layout(this.line, this.column);
        op.src = this.src;
        buf.push(op);
    }
}

export class Include extends Node {
    constructor(line: number, column: number, public src: string) {
        super(line, column);
    }

    compile(buf: opc.Opcode[]) {
        var op = new opc.Include(this.line, this.column);
        op.src = this.src;
        buf.push(op);
    }
}



export class If extends NodeList {
    items: Node[] = []
    constructor(line: number, column: number, public condition: Node) {
        super();
    }

    compile(buf: opc.Opcode[]) {

        let start = new opc.Nop(this.line, this.column);
        let end = new opc.Nop(this.line, this.column);
        //计算条件
        this.condition.compile(buf);
        let go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.FALSE;
        go.target = start;
        buf.push(go);
        //执行块
        super.compile(buf);
        go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.NEVER;
        go.target = end;
        buf.push(go);
        //执行else if
        buf.push(start);
        for (let node of this.items) {
            start = new opc.Nop(this.line, this.column);
            node.compile(buf, start, end);
            buf.push(start);
        }
        buf.push(end);

    }
}

export class Elif extends NodeList {
    constructor(line: number, column: number, public condition: Node) {
        super();
    }

    compile(buf: opc.Opcode[], start: opc.Opcode, end: opc.Opcode) {

        //计算条件
        this.condition.compile(buf);
        let go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.FALSE;
        go.target = start;
        buf.push(go);
        //执行块
        super.compile(buf);
        go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.NEVER;
        go.target = end;
        buf.push(go);
    }
}

export class Else extends NodeList {
    constructor(line: number, column: number) {
        super();
    }

    compile(buf: opc.Opcode[], start: opc.Opcode, end: opc.Opcode) {
        //执行块
        super.compile(buf);
        let go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.NEVER;
        go.target = end;
        buf.push(go);
    }
}

export class Break extends Node {
    constructor(line: number, column: number, public goHead: boolean) {
        super(line, column);
    }

    compile(buf: opc.Opcode[], start: opc.Opcode, end: opc.Opcode) {
        let go = new opc.Goto(this.line, this.column);
        go.flag = opc.Goto.NEVER;
        go.target = this.goHead === true ? start : end;
        buf.push(go);
    }
}

/**
 * Nop
 */
class Nop extends Node {
    constructor() {
        super(0, 0);
    }
}

export class For extends NodeList {
    elseBlock: Node
    constructor(line: number, column: number, public keyName: string, public valueName: string, public iteration: Node) {
        super();
    }

    compile(buf: opc.Opcode[], start: opc.Opcode, end: opc.Opcode) {

        start = new opc.Nop(this.line, this.column);
        end = new opc.Nop(this.line, this.column);

        //获取对象
        this.iteration.compile(buf);
        let ct = new opc.CastToIterator(this.line, this.column);
        buf.push(ct);

        let objName = opc.randomName();//obj
        let hasNextName = opc.randomName();
        let nextName = opc.randomName();

        new Set(this.line, this.column, objName, new Nop()).compile(buf);
        new Set(this.line, this.column, hasNextName, new Property(0, 0, new Identifier(0, 0, objName), new NodeList([new String('hasNext')]))).compile(buf);
        new Set(this.line, this.column, nextName, new Property(0, 0, new Identifier(0, 0, objName), new NodeList([new String('next')]))).compile(buf);

        //开始标记
        buf.push(start);
        //执行方法：
        //没有参数
        let op = new opc.LoadVariable(0, 0);
        op.name = objName;
        buf.push(op);
        op = new opc.LoadVariable(0, 0);
        op.name = hasNextName;
        buf.push(op);
        let call = new opc.Call(0, 0);
        call.parameters = 0;
        buf.push(call);
        //如果结束
        let go = new opc.Goto(0, 0);
        go.flag = opc.Goto.FALSE;
        go.target = end;
        buf.push(go);

        new Set(this.line, this.column, this.valueName, new Property(0, 0, new Identifier(0, 0, objName), new NodeList([new String('current')]))).compile(buf);

        //执行下一个迭代
        //没有参数
        op = new opc.LoadVariable(0, 0);
        op.name = objName;
        buf.push(op);
        op = new opc.LoadVariable(0, 0);
        op.name = nextName;
        buf.push(op);
        call = new opc.Call(0, 0);
        call.parameters = 0;
        buf.push(call);

        super.compile(buf, start, end); //需要传递开始与结束标签，上下文中 break,continue语句需要使用

        //回到开始
        go = new opc.Goto(0, 0);
        go.flag = opc.Goto.NEVER;
        go.target = start;
        buf.push(go);
        //结束
        buf.push(end);
        //TODO: 删除临时方法
    }
}