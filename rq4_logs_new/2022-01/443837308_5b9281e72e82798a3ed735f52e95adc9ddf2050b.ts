import { Injectable } from "@angular/core";
import { ActivatedRouteSnapshot, CanDeactivate as NgCanDeactivate, RouterStateSnapshot } from "@angular/router";
import { Observable } from "rxjs";
import { ProvidedIn } from "../enums/ProvidedIn";

export interface CanDeactivate {
  canDeactivate(
    currentRoute: ActivatedRouteSnapshot,
    currentState: RouterStateSnapshot,
    nextState?: RouterStateSnapshot
  ): Observable<boolean> | boolean
}

@Injectable({
  providedIn: ProvidedIn.ROOT
})
export class PendingChangesGuard implements NgCanDeactivate<CanDeactivate> {
  canDeactivate(
    component: CanDeactivate,
    currentRoute: ActivatedRouteSnapshot,
    currentState: RouterStateSnapshot,
    nextState?: RouterStateSnapshot
  ) {
    return component.canDeactivate(
      currentRoute,
      currentState,
      nextState
    )
  }
}