//Feito por Felipe Merci Fantin
export class ControleDeNota {
    constructor(
        private _resultado: number,
        private _nota1: number,
        private _nota2: number,

    ) { }


    public get resultado(): number {
        return this._resultado;
    }

    public set resultado(resultado: number) {
        this._resultado = resultado;
    }

    public get nota1(): number {
        return this._nota1;
    }

    public set nota1(nota1: number) {
        this._nota1 = nota1;
    }
    public get nota2(): number {
        return this._nota2;
    }


    public set nota2(nota2: number) {
        this._nota2 = nota2;
    }


    permitida(
        n1: number, n2: number): void {

        this.nota1 = n1;
        this.nota2 = n2;

    }

    prova(): void {
        this.resultado = (this.nota1 + this.nota2) / 2;
        if (this.resultado <= 59.9) {

            console.log('Você Reprovou');
            console.log('');
            console.log('Nota para passar: 60 ');

        } else {

            console.log('Você passou ');
            console.log('');
            console.log('Nota para passar: 60 ');

        }

        console.log('');
        console.log('Nota Média do Aluno : ' + this.resultado);
        console.log('');

    }

}