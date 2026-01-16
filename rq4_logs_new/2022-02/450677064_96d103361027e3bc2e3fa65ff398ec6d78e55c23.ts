import ICompte from '../interfaces/ICompte';
export default class Compte {

    public idCompte: number;
    public noEntreprise: number;
    public isAdmin: boolean;
    public mail: string;
    public pwd: string;
    public name: string;
    public firstName: string;
    public isValid: boolean;
    public creationDate: Date;
    public lastLogin: Date;
    public isCgu: boolean;
    public isForgotpass: boolean;
    public isTempPassword: string;


    constructor(
        idCompte: number,
        noEntreprise: number,
        isAdmin: boolean,
        mail: string,
        pwd: string,
        name: string,
        firstName: string,
        isValid: boolean,
        creationDate: Date,
        lastLogin: Date,
        isCgu: boolean,
        isForgotpass: boolean,
        isTempPassword: string
    ){
        this.idCompte = idCompte;
        this.noEntreprise = noEntreprise;
        this.isAdmin = isAdmin;
        this.mail = mail;
        this.pwd = pwd;
        this.name = name;
        this.firstName = firstName;
        this.isValid = isValid;
        this.creationDate = creationDate;
        this.lastLogin = lastLogin;
        this.isCgu = isCgu;
        this.isForgotpass = isForgotpass;
        this.isTempPassword = isTempPassword;
    }

    public static toCompte(iCompte: ICompte): Compte{
        return new Compte(
            iCompte.idCompte,
            iCompte.noEntreprise,
            iCompte.isAdmin,
            iCompte.mail,
            iCompte.pwd,
            iCompte.name,
            iCompte.firstName,
            iCompte.isValid,
            iCompte.creationDate,
            iCompte.lastLogin,
            iCompte.isCgu,
            iCompte.isForgotpass,
            iCompte.isTempPassword
        );
    }

    public toJSON(): ICompte{
        return {
            "idCompte": this.idCompte,
            "noEntreprise": this.noEntreprise,
            "isAdmin": this.isAdmin,
            "mail": this.mail,
            "pwd": this.pwd,
            "name": this.name,
            "firstName": this.firstName,
            "isValid": this.isValid,
            "creationDate": this.creationDate,
            "lastLogin": this.lastLogin,
            "isCgu": this.isCgu,
            "isForgotpass": this.isForgotpass,
            "isTempPassword": this.isTempPassword
        }
    }
}