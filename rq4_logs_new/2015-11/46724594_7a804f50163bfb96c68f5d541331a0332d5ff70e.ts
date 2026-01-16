//admin-router.ts
///
import {Router} from 'aurelia-router';
import {BaseModel} from '../data/basemodel';
import {UserInfo} from '../data/userinfo';
//
export class AdminRouter extends BaseModel {
	//
	public static inject() { return [UserInfo]; }
	//
	public heading: string = 'Administration';
	public router: Router;
	//
	constructor(info: UserInfo) {
		super(info);
		this.title = 'Administration';
	}// constructor
	//
	public configureRouter(config, router: Router) {
		config.map([
			{ route: ['', 'home'], moduleId: '../home', nav: true, title: 'Accueil' },
			{ route: 'semestres', moduleId: './semestres', nav: true, title: 'Semestres' },
			{ route: 'annees', moduleId: './annees', nav: true, title: 'Années' },
			{ route: 'departements', moduleId: './departements', nav: true, title: 'Départements' }
			/*
			{ route: 'etudaffectations', moduleId: './etudaffectations', nav: true, title: 'Affectations Etudiants' },
			{ route: 'importetuds', moduleId: './import-etuds', nav: true, title: 'Import Etudiants' },
			{ route: 'profaffectations', moduleId: './profaffectations', nav: true, title: 'Affectations Enseignants' },
			{ route: 'etudiants', moduleId: './etudiants', nav: true, title: 'Etudiants' },
			
			
			{ route: 'enseignants', moduleId: './enseignants', nav: true, title: 'Enseignants' },
			{ route: 'groupes', moduleId: './groupes', nav: true, title: 'Groupes' },
			{ route: 'matieres', moduleId: './matieres', nav: true, title: 'Matières' },
			{ route: 'unites', moduleId: './unites', nav: true, title: 'Unités' },
			{ route: 'administrators', moduleId: './administrators', nav: true, title: 'Opérateurs' },
			
			*/
			/*
			{ route: 'etud/:id', name: 'etud', moduleId:'../consult/etudiants-sumary', nav: false }
			*/
		]);
		this.router = router;
	}
	public canActivate(params?: any, config?: any, instruction?: any): any {
		let bRet: boolean = false;
		if (this.is_connected) {
			bRet = this.is_admin || this.is_super;
		}
		return bRet;
	}// activate
}