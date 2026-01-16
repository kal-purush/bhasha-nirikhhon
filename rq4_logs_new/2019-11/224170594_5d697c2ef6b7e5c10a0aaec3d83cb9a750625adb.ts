import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Resolve } from '@angular/router';
import { Observable, of } from 'rxjs';
import { Empresa } from '../empresa';
import { EmpresasService } from '../empresas.service';

@Injectable({
  providedIn: 'root'
})
export class EmpresaResolverGuard implements Resolve<Empresa> {

  constructor(private empresaService: EmpresasService) { }

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<Empresa> {

    if (route.params && route.params['id']) {
      return this.empresaService.findById(route.params['id']);
    }

    // retorna um Observalbe a partir de um objeto
    return of({
      id: null,
      nome: null,
      endereco: null,
      cnpj: null,
    });
  }
}