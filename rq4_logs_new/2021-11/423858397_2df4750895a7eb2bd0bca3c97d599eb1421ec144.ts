export class OfferForm {

    //Declaro la clase formulario de registro con todos los campos que tendrá mi formulario
    private _titulo: string;
    private _descripcion: string;
    private _empresa: string;
    private _salario: string;
    private _ciudad: string;
    private _email: string;


    constructor(titulo: string, descripcion: string, empresa: string, salario: string, ciudad: string, email: string ) {
            this._titulo = titulo;
            this._descripcion = descripcion;
            this._empresa = empresa;
            this._salario = salario;
            this._ciudad = ciudad;
            this._email = email
        }

        //Getters y Setters
        get titulo(): string {
            return this._titulo;
        }
        set titulo(value: string) {
            this._titulo = value;
        }
        
        get descripcion(): string {
            return this._descripcion;
        }
        set descripcion(value: string) {
            this._descripcion = value;
        }
        
        get empresa(): string {
            return this._empresa;
        }
        set empresa(value: string) {
            this._empresa = value;
        }
        
        get salario(): string {
            return this._salario;
        }
        set salario(value: string) {
            this._salario = value;
        }

        get ciudad(): string {
            return this._ciudad;
        }
        set ciudad(value: string) {
            this._ciudad = value;
        }

        get email(): string {
            return this._email;
        }
        set email(value: string) {
            this._email = value;
        }
    
}