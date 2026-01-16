"use strict";
class Calculadora {
    constructor() {
        this.estado = 1;

    }
    calc() {
        try {
            this.resultado = eval(this.resultado);

            this.estado = 0;
        } catch (e) {
            console.log('la expresion compuesta no es computable. borra y empieza de nuevo');
            this.estado = -1;
            this.resultado = 'ERROR'
        }

    }
    display(number) {
        if (this.estado <= 0) {
            this.estado = 1;
            this.resultado = "";
        }
        this.resultado += number;
    }

    displayOp(operation) {
        if (this.estado < 0) {
            this.resultado = "";
        }
        this.estado = 1;
        this.resultado += operation;
    }
    c() {

        this.estado = 1;
        this.resultado = "";

    }

}



module.exports = Calculadora;