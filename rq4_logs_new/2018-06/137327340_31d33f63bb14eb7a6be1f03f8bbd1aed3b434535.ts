import { ActivatedRouteSnapshot, Resolve, RouterStateSnapshot } from "@angular/router";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { Formation } from "../entity/formation";

@Injectable()
export class FormationsResolvers implements Resolve<any> {

    constructor() {
    }


    resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any> | Promise<any> | any {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve ([
                    new Formation("Spring"),
                    new Formation("Angular"),
                    new Formation("Java")
                ]);
            }, 5000);
      });
    }
}