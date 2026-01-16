import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve, RouterStateSnapshot } from '@angular/router';

import { Store } from '@ngrx/store';

import { SelectCharacterAction } from './character.actions';

@Injectable()
export class CharacterIdResolver implements Resolve<string> {

    constructor(private store: Store<any>) {}

    resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): string {
        const id = route.paramMap.get('id');
        this.store.dispatch(new SelectCharacterAction(id));
        return id;
    }

}