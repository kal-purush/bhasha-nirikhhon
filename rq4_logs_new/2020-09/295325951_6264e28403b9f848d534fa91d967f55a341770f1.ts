import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Table } from '../../st/Table';
import { ExceptionST } from '../../st/ExceptionST';
import { TypeError } from '../../st/TypeError';
import { ContinueNode } from '../../nodes/Expresiones/ContinueNode';
import { BreakNode} from '../../nodes/Expresiones/BreakNode';

const parser = require('../../parser/grammar.js');

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private urlExecute = '/ejecutar';
  constructor(private _http: HttpClient) {}

  analizar(cadena: any): Observable<any> {
   /* const tree = parser.parse(cadena);
    const tabla = new Table(null);

    tree.instructions.map((m: any) => {
      const res = m.execute(tabla, tree);

      if (res instanceof BreakNode) {
        const error = new ExceptionST(TypeError.SEMANTICO,
          `Sentencia break fuera de un ciclo`,
          res.line, res.column);
        tree.excepciones.push(error);
        tree.console.push(error.toString());
      } else if (res instanceof ContinueNode) {
        const error = new ExceptionST(TypeError.SEMANTICO,
          `Sentencia continue fuera de un ciclo`,
          res.line, res.column);
        tree.excepciones.push(error);
        tree.console.push(error.toString());
      }
    });*/

    return this._http.post(this.urlExecute,   cadena);
  }
}