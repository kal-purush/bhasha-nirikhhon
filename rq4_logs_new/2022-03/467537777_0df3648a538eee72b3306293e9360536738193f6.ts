import { Injectable } from "@angular/core";
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot, UrlTree } from "@angular/router";
import { Observable } from "rxjs";
import { CompleteProcessingService } from "../services/completeProcessing.service";
import { ConfirmReturnService } from "./confirmReturn.service";

@Injectable({
    providedIn: 'root'
})

export class ConfirmGuard implements CanActivate {

    constructor(private confirmReturnService: ConfirmReturnService, private router: Router, private ompleteProcessingService: CompleteProcessingService) { }
    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean | UrlTree | Observable<boolean | UrlTree> | Promise<boolean | UrlTree> {

        if (this.confirmReturnService.getResponse())
            return true;
        else {
            return this.router.createUrlTree(['/home'])
        }
    }
}