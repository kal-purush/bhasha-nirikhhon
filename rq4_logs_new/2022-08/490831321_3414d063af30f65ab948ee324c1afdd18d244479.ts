export class PaymentModel{
    constructor(
        public tarjeta:string,
        public cvv:string,
        public fecha:string,
        public terminoscondiciones:boolean
    ){}
}