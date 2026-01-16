export class User {
    private id: number;
    private name: string;
    private email: string;
    private phone1: string;
    private phone2: string;
    private address: string;
    private adressNumber: string;
    private bairro: string;
    private zipCode: string;

    public constructor(
        id: number,
        name: string, 
        email: string, phone1: string,
        phone2: string,
        address: string,
        adressNumber: string,
        bairro: string,
        zipCode: string
        ){
            this.id = id;
            this.name = name;
            this.email = email;
            this.phone1 = phone1;
            this.phone2 = phone2;
            this.address = address;
            this.adressNumber = adressNumber;
            this.bairro = bairro;
            this.zipCode = zipCode; 
        }

}