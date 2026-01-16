export class LoginForm {
    //Declaro la clase formulario de login con todos los campos que tendrá mi formulario de login

    private _nombre: string;
    private _pass: string;
    private _check: boolean;
    
    constructor(nombre: string, pass: string, check: boolean) {
            this._nombre = nombre;
            this._pass = pass;
            this._check = check;
        }
    
    get nombre(): string {
        return this._nombre;
    }
    set nombre(value: string) {
        this._nombre = value;
    }

    get pass(): string {
        return this._pass;
    }
    set pass(value: string) {
        this._pass = value;
    }

    get check(): boolean {
        return this._check;
    }
    set check(value: boolean) {
        this._check = value;
    }
    
}