module Models{
    export class user{
        name: string;
        country: string;
        latitude: number;
        longitude: number;

        constructor(name:string, country:string){
            this.name = name;
            this.country = country;
        }
    }
} 