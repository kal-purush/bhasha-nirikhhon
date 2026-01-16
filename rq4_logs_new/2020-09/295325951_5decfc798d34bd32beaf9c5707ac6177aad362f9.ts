import { Node } from '../Node';
import { Exception } from '../../st/Exception';
import { types, Type } from '../../st/Type';
import { Table } from '../../st/Table';
import { Tree } from '../../st/Tree';

export class ArithNode extends Node {
  arg1: Node;
  arg2: Node;
  op: String;

  constructor(
    arg1: Node,
    arg2: Node,
    op: String,
    line: Number,
    column: Number
  ) {
    super(null, line, column);
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.op = op;
  }

  execute( table: Table, tree: Tree ) {
    const lResult = this.arg1.execute( table, tree );
    if (lResult instanceof Exception) {
      return lResult;
    }
    const rResult = this.arg2.execute( table , tree );
    if (rResult instanceof Exception) {
      return rResult;
    }

    switch (this.op) {
      case '+':
        if (
          this.arg1.type.type === types.NUMBER && this.arg2.type.type === types.NUMBER
        ) {
          this.type = new Type(types.NUMBER);
          return lResult + rResult;
        } else if (
          this.arg1.type.type === types.STRING && this.arg2.type.type === types.STRING
        ) {
          this.type = new Type(types.STRING);
          return lResult + rResult;
        } else if (
          this.arg1.type.type === types.ARRAY && this.arg2.type.type === types.ARRAY
        ) {
          // ARRAYS


        } else {
          const error = new Exception(
            'Semantico',
            `Error en los tipo - Type mismatch'  ${this.arg1.type.type} y ${this.arg2.type.type}`,
            this.line,
            this.column
          );
          return error;
        }

      case '-':
        if (this.arg1.type.type === types.NUMBER && this.arg2.type.type === types.NUMBER) {
          this.type = new Type(types.NUMBER);
          return lResult  - rResult;
      } else {
          console.log(this.arg1);
          const error = new Exception('Semantico',
              `Error de tipos en la resta se esta tratando de operar ${this.arg1.type.type} y ${this.arg2.type.type}`,
              this.line, this.column);
          tree.excepciones.push(error);
          tree.console.push(error.toString());
          return error;
      }
      case '*':
        if (this.arg1.type.type === types.NUMBER && this.arg2.type.type === types.NUMBER) {
          this.type = new Type(types.NUMBER);
          return lResult * rResult;
      } else {
          const error = new Exception('Semantico',
              `Error de tipos en la multiplicacion se esta tratando de operar ${this.arg1.type.type} y ${this.arg2.type.type}`,
              this.line, this.column);
          tree.excepciones.push(error);
          tree.console.push(error.toString());
          return error;
      }
      case '/':
        if (this.arg1.type.type === types.NUMBER && this.arg2.type.type === types.NUMBER) {
          this.type = new Type(types.NUMBER);
          if (rResult === 0) {
              const error = new Exception('Semantico',
                  `Error aritmetico, La division con cero no esta permitida`,
                  this.line, this.column);
              tree.excepciones.push(error);
              tree.console.push(error.toString());
              return error;
          }
          return lResult / rResult;
        }else {
          const error = new Exception('Semantico',
              `Error de tipos en la division se esta tratando de operar ${this.arg1.type.type} y ${this.arg2.type.type}`,
              this.line, this.column);
          tree.excepciones.push(error);
          tree.console.push(error.toString());
          return error;
      }
      case '--':
        if (this.arg1.type.type === types.NUMBER) {
          this.type = new Type(types.NUMBER);
          return lResult * -1;
      } else {
          const error = new Exception('Semantico',
              `Error de tipos en el operador unario se esta tratando de operar ${this.arg1.type.type}`,
              this.line, this.column);
          tree.excepciones.push(error);
          tree.console.push(error.toString());
          return error;
      }

      default:
        const error = new Exception('Semantico',
        `Error, Operador desconocido`,
        this.line, this.column);
    tree.excepciones.push(error);
    tree.console.push(error.toString());
    return error;
    }
  }
}