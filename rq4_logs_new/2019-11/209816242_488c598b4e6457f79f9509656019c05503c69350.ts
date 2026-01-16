/**
 * @author Hadi Horani, s144885
 */

import { Injectable, OnInit } from '@angular/core';
import { CanActivate, Router, ActivatedRouteSnapshot } from '@angular/router';
import { AuthService } from '../auth.service';
import { Select } from '@ngxs/store';
import { TokenState } from 'src/app/_states/token.state';
import { Observable } from 'rxjs';
import { User } from 'src/app/models/login/user.model';
import { tap, map } from 'rxjs/operators';

@Injectable()
export class AuthRouteGuard implements CanActivate
{

  @Select( TokenState.getUser ) user$: Observable<User>;


  constructor ( private auth: AuthService, private router: Router )
  {
  }

  canActivate ( route: ActivatedRouteSnapshot ): Observable<boolean>
  {
    if ( !this.auth.isAuthenticated() )
    {
      this.auth.authenticate();
    }
    return this.userIsInRole( route );
  }

  userIsInRole ( route: ActivatedRouteSnapshot ): Observable<boolean>
  {

    const expectedRole = route.data.expectedRole.split( /[ ,]+/ );

    return this.user$.pipe( map( user =>
    {

      if ( expectedRole.filter( e => user.roles.includes( e ) ).length === 0 )
      {
        alert( 'Not allowed' );
        return false;
      } else
      {
        alert( 'allow' )
        return true;
      }
    }
    )
    )
  }
}