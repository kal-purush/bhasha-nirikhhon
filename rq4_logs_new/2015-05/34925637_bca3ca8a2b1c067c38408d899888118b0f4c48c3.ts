//workviewmodel.ts
//
//
import {UserInfo} from './userinfo';
import {IDepartement, IAnnee, IUnite,ISemestre, IMatiere, IGroupe} from '../../infodata.d';
import {InfoRoot} from '../../inforoot';
import {BaseViewModel} from './baseviewmodel';
//
export class WorkViewModel extends BaseViewModel {
    //
    private _departements: IDepartement[] = null;
    private _departement: IDepartement = null;
    private _annees: IAnnee[] = null;
    private _annee: IAnnee = null;
    private _unites: IUnite[] = null;
    private _unite: IUnite = null;
    private _semestres: ISemestre[] = null;
    private _semestre: ISemestre = null;
    private _matieres: IMatiere[] = null;
    private _matiere: IMatiere = null;
    private _groupes: IGroupe[] = null;
    private _groupe: IGroupe = null;
    //
    constructor(userinfo: UserInfo) {
        super(userinfo);
    }// constructor
    //
    public canActivate(params?: any, config?: any, instruction?: any): any {
        let px = this.userInfo.person;
        return (px !== null) && (px.id !== null);
    }// activate
    //
    public activate(params?: any, config?: any, instruction?: any): any {
        let self = this;
        return super.activate(params, config, instruction).then((r) => {
            return self.initialize_data();
        });
    }// activate
    //
    protected initialize_data(): Promise<any> {
        let self = this;
        let userinfo = this.userInfo;
        return userinfo.departements.then((dd) => {
            self._departements = ((dd !== undefined) && (dd !== null)) ? dd : [];
            let id = self.userInfo.departementid;
            let p = InfoRoot.sync_array(self.departements, id);
            self.departement = p;
            return true;
        });
    }// initialize_data
    protected post_change_departement(): Promise<any> {
        return Promise.resolve(true);
    }
    protected post_change_unite(): Promise<any> {
        return Promise.resolve(true);
    }
    protected post_change_annee(): Promise<any> {
        return Promise.resolve(true);
    }
    protected post_change_groupe(): Promise<any> {
        return Promise.resolve(true);
    }
    protected post_change_semestre(): Promise<any> {
        return Promise.resolve(true);
    }
    protected post_change_matiere(): Promise<any> {
        return Promise.resolve(true);
    }
    //
    public get departements(): IDepartement[] {
        return (this._departements !== null) ? this._departements : [];
    }
    public get departement(): IDepartement {
        return this._departement;
    }
    public set departement(s: IDepartement) {
        this._departement = (s !== undefined) ? s : null;
        let id = (this.departement !== null) ? this.departement.id : null;
        this.userInfo.departementid = id;
        this._unites = null;
        this._unite = null;
        this._groupes = null;
        this._groupe = null;
        this._annees = null;
        this._annee = null;
        this._semestres = null;
        this._semestre = null;
        this._matieres = null;
        this._matiere = null;
        let self = this;
        let userinfo = this.userInfo;
        userinfo.annees.then((aa) => {
            self._annees = ((aa !== undefined) && (aa !== null)) ? aa : [];
            return userinfo.unites;
        }).then((uu) => {
            self._unites = ((uu !== undefined) && (uu !== null)) ? uu : [];
            return userinfo.groupes;
        }).then((gg) => {
            self._groupes = ((gg !== undefined) && (gg !== null)) ? gg : [];
            return self.post_change_departement();
        });
    }
    public get departementid(): string {
        return (this.departement !== null) ? this.departement.id : null;
    }
    //
    public get annees(): IAnnee[] {
        return (this._annees !== null) ? this._annees : [];
    }
    public get annee(): IAnnee {
        return this._annee;
    }
    public set annee(s: IAnnee) {
        this._annee = (s !== undefined) ? s : null;
        let id = (this.annee !== null) ? this.annee.id : null;
        this.userInfo.anneeid = id;
        this._semestres = null;
        this._semestre = null;
        let self = this;
        let userinfo = this.userInfo;
        userinfo.semestres.then((mm) => {
            self._semestres = ((mm !== undefined) && (mm !== null)) ? mm : [];
            return self.post_change_annee();
        });
    }
    public get anneeid(): string {
        return (this.annee !== null) ? this.annee.id : null;
    }
    //
    public get unites(): IUnite[] {
        return (this._unites !== null) ? this._unites : [];
    }
    public get unite(): IUnite {
        return this._unite;
    }
    public set unite(s: IUnite) {
        this._unite = (s !== undefined) ? s : null;
        let id = (this.unite !== null) ? this.unite.id : null;
        this.userInfo.uniteid = id;
        this._matieres = null;
        this._matiere = null;
        let self = this;
        let userinfo = this.userInfo;
        userinfo.matieres.then((mm) => {
            self._matieres = ((mm !== undefined) && (mm !== null)) ? mm : [];
            return self.post_change_unite();
        });
    }
    public get uniteid(): string {
        return (this.unite !== null) ? this.unite.id : null;
    }
    //
    public get semestres(): ISemestre[] {
        return (this._semestres !== null) ? this._semestres : [];
    }
    public get semestre(): ISemestre {
        return this._semestre;
    }
    public set semestre(s: ISemestre) {
        this._semestre = (s !== undefined) ? s : null;
        let id = (this.semestre !== null) ? this.semestre.id : null;
        this.userInfo.semestreid = id;
        this.post_change_semestre();
    }
    public get semestreid(): string {
        return (this.semestre !== null) ? this.semestre.id : null;
    }
    //
    public get groupes(): IGroupe[] {
        return (this._groupes !== null) ? this._groupes : [];
    }
    public get groupe(): IGroupe {
        return this._groupe;
    }
    public set groupe(s: IGroupe) {
        this._groupe = (s !== undefined) ? s : null;
        let id = (this.groupe !== null) ? this.groupe.id : null;
        this.userInfo.groupeid = id;
        this.post_change_groupe();
    }
    public get groupeid(): string {
        return (this.groupe !== null) ? this.groupe.id : null;
    }
    //
    public get matieres(): IMatiere[] {
        return (this._matieres !== null) ? this._matieres : [];
    }
    public get matiere(): IMatiere {
        return this._matiere;
    }
    public set matiere(s: IMatiere) {
        this._matiere = (s !== undefined) ? s : null;
        let id = (this.matiere !== null) ? this.matiere.id : null;
        this.userInfo.matiereid = id;
        this.post_change_matiere();
    }
    public get matiereid(): string {
        return (this.matiere !== null) ? this.matiere.id : null;
    }
}// class BaseEditViewModel