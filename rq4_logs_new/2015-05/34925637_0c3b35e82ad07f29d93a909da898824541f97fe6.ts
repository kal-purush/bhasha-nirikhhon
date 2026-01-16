//admin-router.ts
/// <reference path='../../../../typings/aurelia/aurelia.d.ts' />
//
import {IRouterConfig, Router} from 'aurelia-router';
//
const NOT_IMPLEMENTED = '../not-implemented';
//
export class AdminRouter {
    public router: Router;
    public heading: string = 'Administration';
    configureRouter(config: IRouterConfig, router: Router) {
        config.map([
            { route: ['', 'home'], moduleId: '../../../home', nav: true, title: 'Accueil' },
            { route: 'affetuds', moduleId: NOT_IMPLEMENTED, nav: true, title:'Affectations étudiants' },
            { route: 'affprofs', moduleId: NOT_IMPLEMENTED, nav: true, title:'Affectations enseignants' },
            { route: 'etudiants', moduleId: NOT_IMPLEMENTED, nav: true, title:'Etudiants' },
            { route: 'semestres', moduleId: './semestres', nav: true, title:'Semestres' },
            { route: 'enseignants', moduleId: NOT_IMPLEMENTED, nav: true, title:'Enseignants' },
            { route: 'annees', moduleId: './annees', nav: true, title:'Annees' },
            { route: 'matieres', moduleId: './matieres', nav: true, title:'Matières' },
            { route: 'unites', moduleId: './unites', nav: true, title:'Unités' },
            { route: 'groupes', moduleId: './groupes', nav: true, title:'Groupes' },
            { route: 'departements', moduleId: './departements', nav: true, title:'Départements' }
        ]);
        this.router = router;
    }
}