import { Injectable } from '@angular/core';
import { Router, Resolve, RouterStateSnapshot,
         ActivatedRouteSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';

import { CharacterService } from './character.service';
import { GamerTagService } from '../../services/gamer-tag/gamer-tag.service';
import { Gamer } from '../../models/gamer.interface';

@Injectable()
export class InventoryResolver implements Resolve<any> {
    gamer: Gamer;
    constructor(private charService: CharacterService ,
                private router: Router,
                private gamerInfo: GamerTagService
               ) { this.gamer = this.gamerInfo.get()}

    resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any> {
      this.gamer = Object.assign({}, this.gamer, { characterId: route.params.id });
      return  this.charService.inventory(this.gamer);
    }
}