import { Node } from '../Node';
import { Table } from '../../st/Table';
import { Tree } from '../../st/Tree';
import { ExceptionST } from '../../st/ExceptionST';
import { TypeError } from '../../st/TypeError';

export class ReturnNode extends Node {
  value: Node;

  constructor(value: Node, line: Number, column: Number) {
    super(null, line, column);
    this.value = value;
  }

  execute(table: Table, tree: Tree) {
    if (this.value !== null) {
      // RETURN UNA EXPRESIÓN
      const result = this.value.execute(table, tree);
      if (result instanceof ExceptionST) {
        return result;
      }

      if (result === null) {
        const error = new ExceptionST(
          TypeError.SEMANTICO,
          'Este valor No se puede retornar',
          this.line,
          this.column
        );
        tree.excepciones.push(error);
        tree.console.push(error.toString());
        return error;
      } else {
        return result;
      }
    } else {
      //RETURN SIN EXP
      return this;
    }
  }
}