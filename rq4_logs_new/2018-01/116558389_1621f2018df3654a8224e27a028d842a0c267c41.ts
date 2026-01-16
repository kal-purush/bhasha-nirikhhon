import { Injectable } from '@angular/core';
import { Router, CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { MarvelService } from '../services/marvel.service';

@Injectable()
export class CharacterGuard implements CanActivate {

  constructor(
    private _router: Router,
    private _marvelService: MarvelService) {}

  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
      if (!this._marvelService.selectedCharacter) {
        this._router.navigate(['']);
        return false;
      }
      return true;
  }
}