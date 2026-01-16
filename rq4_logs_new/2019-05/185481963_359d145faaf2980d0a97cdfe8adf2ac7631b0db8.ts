import {CanActivate, Router} from '@angular/router';
import {Injectable} from '@angular/core';
import {ActivatedRouteSnapshot, RouterStateSnapshot} from '@angular/router/src/router_state';
import { UsersService } from '../services/users.service';

@Injectable()




export class Auth implements CanActivate{


    constructor(private router: Router, private usersService: UsersService ){

    }

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot){

        const redirectUrl = route['_routerState']['/main'];
        if(this.usersService.isLogged()){
            return true;
        }

        // this.router.navigate(
        //     this.router.createUrlTree(
        //         ['/login'],{
        //             queryParams:{
        //                 redirectUrl
        //             }
        //         }
        //     )
        // );
        return false;
    }
}
