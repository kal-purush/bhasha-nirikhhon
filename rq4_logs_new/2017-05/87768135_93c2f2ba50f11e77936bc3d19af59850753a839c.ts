import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { CommandService } from '../shared/command.service';

@Injectable()
export class CanActivateCommand implements CanActivate {
  constructor(private commandService: CommandService, private router: Router) {
  }

  canActivate(
    route: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ): Observable<boolean>|Promise<boolean>|boolean {
    const commandName =  route.params.name;
    const exists = !!this.commandService.find(commandName);

    if (!exists) {
      this.router.navigate(['/commands']);
    }

    return exists;
  }
}