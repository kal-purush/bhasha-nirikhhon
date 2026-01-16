import { Injectable } from "@angular/core";
import {
  CanActivate,
  ActivatedRouteSnapshot,
  RouterStateSnapshot,
  UrlTree,
  Resolve
} from "@angular/router";
import { Observable, of } from "rxjs";
import { FuncionariosService } from "../funcionarios.service";
import { Funcionario } from "../funcionarios";

@Injectable({
  providedIn: "root"
})
export class funcionarioResolverGuard implements Resolve<Funcionario> {
  constructor(private funcionarioService: FuncionariosService) {}

  resolve(
    route: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ): Observable<Funcionario> {
    if (route.params && route.params["id"]) {
      return this.funcionarioService.findById(route.params["id"]);
    }

    // retorna um Observalbe a partir de um objeto
    return of({
      id: null,
      nome: null,
      dataNascimento: null,
      cpf: null,
      empresa: null
    });
  }
}