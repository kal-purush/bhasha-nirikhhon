//baseviewmodel.ts
//
import {UserInfo} from './userinfo';
import {IDataService, IPerson} from '../../infodata.d';
import {InfoRoot} from '../../inforoot';
//
export class BaseViewModel {
    //
    private _user: UserInfo;
    public title: string;
    public errorMessage: string;
    public infoMessage: string;
    //
    constructor() {
        this._user = null;
        this.title = null;
        this.errorMessage = null;
        this.infoMessage = null;
    }// constructor
    public get userInfo(): UserInfo {
        if (this._user === null) {
            this._user = new UserInfo();
        }
        return this._user;
    }// get userInfo
    public get dataService(): IDataService {
        return this.userInfo.dataService;
    }
    public get person(): IPerson {
        return this.userInfo.person;
    }
    public get isConnected(): boolean {
        let x = this.person;
        return (x !== null) && (x.id !== null);
    }// isConnected
    public set isConnected(s: boolean) {
    }
    public get isNotConnected(): boolean {
        return (!this.isConnected);
    }
    public set isNotConnected(s: boolean) {
    }
    public activate(params?: any, config?: any, instruction?: any): any {
        this.update_title();
    }// activate
    protected update_title(): any {
    } // update_title
    public get hasErrorMessage(): boolean {
        return (this.errorMessage !== null) && (this.errorMessage.trim().length > 0);
    }
    public set hasErrorMessage(b: boolean) {
    }
    public get hasInfoMessage(): boolean {
        return (this.infoMessage !== null) && (this.infoMessage.trim().length > 0);
    }
    public set hasInfoMessage(b: boolean) {
    }
    public clear_error(): void {
        this.errorMessage = null;
        this.hasInfoMessage = null;
    }
    public set_error(err: any): void {
        if ((err !== undefined) && (err !== null)) {
            if ((err.message !== undefined) && (err.message !== null)) {
                this.errorMessage = (err.message.length > 0) ? err.message : 'Erreur inconnue...';
            } else if ((err.msg !== undefined) && (err.msg !== null)) {
                this.errorMessage = (err.msg.length > 0) ? err.msg : 'Erreur inconnue...';
            } else if ((err.reason !== undefined) && (err.reason !== null)) {
                this.errorMessage = err.reason;
            } else {
                this.errorMessage = JSON.stringify(err);
            }
        } else {
            this.errorMessage = 'Erreur inconnue...';
        }
    } // set_error
    public get fullname(): string {
        let x = this.person;
        return (x !== null) ? x.fullname : null;
    }
    public get photoUrl(): string {
        let x = this.person;
        return (x !== null) ? x.url : null;
    }
    public get hasPhoto(): boolean {
        let x = this.photoUrl;
        return (x !== null);
    }
    public set hasPhoto(s: boolean) { }
    public get isSuper(): boolean {
        let x = this.person;
        return (x !== null) && x.is_super;
    }
    public set isSuper(b: boolean) { }
    public get isAdmin(): boolean {
        let x = this.person;
        return (x !== null) && x.is_admin;
    }
    public set isAdmin(b: boolean) { }
    public get isProf(): boolean {
        let x = this.person;
        return (x !== null) && x.is_prof;
    }
    public set isProf(b: boolean) { }
    public get isEtud(): boolean {
        let x = this.person;
        return (x !== null) && x.is_etud;
    }
    public set isEtud(b: boolean) { }
}// class BaseViewModel