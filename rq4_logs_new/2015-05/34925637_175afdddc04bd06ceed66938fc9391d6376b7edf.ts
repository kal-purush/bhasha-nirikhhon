//prof-router.ts
/// <reference path='../../../../typings/aurelia/aurelia-dependency-injection.d.ts' />
/// <reference path='../../../../typings/aurelia/aurelia-router.d.ts' />
//
import {inject} from 'aurelia-dependency-injection';
import {RouterConfiguration, Router} from 'aurelia-router';
import {BaseViewModel} from '../baseviewmodel';
import {UserInfo} from '../userinfo';
//
const NOT_IMPLEMENTED = '../not-implemented';
//
export class ProfRouter extends BaseViewModel {
    public router: Router = null;
    public heading: string = 'Consultation';
    //
    public static inject() { return [UserInfo]; }
    //
    constructor(userinfo: UserInfo) {
        super(userinfo);
    }// constructor
    //
    public canActivate(params?: any, config?: any, instruction?: any): any {
        let px = this.userInfo.person;
        return (px !== null) && (px.is_prof || px.is_admin);
    }// activate
    //
    public configureRouter(config: RouterConfiguration, router: Router): any {
        config.map([
            { route: ['', 'home'], moduleId: '../home', nav: true, title: 'Accueil' },
            { route: 'profgroupeevents', moduleId: './profgroupeevents', nav: true, title: 'Evènements' }
        ]);
        this.router = router;
    }
} // class ProfRouter