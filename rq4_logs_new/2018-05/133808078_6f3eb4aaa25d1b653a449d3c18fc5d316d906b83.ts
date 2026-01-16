import { Time } from "@angular/common";

export class Annonce {
    adresseDepart:string;
    adresseArriver:string;
    heure:DateTimeFormat;
    vehicule:Vehicule;
    conducteur:Personne;
    duree:Time;
    distance:number;
    placeDispo:number;
    constructor(){

    }
}

export enum Categorie {
    MICRO_URBAINE = "Micro-urbaines", MINI_CITADINE="Mini-citadines", 
    CITADINE_POLYVALENTE = "Citadines polyvalentes",
    COMPACTES = "Compactes", 
    BERLINE_S = "Berlines Taille S",
    BERLINE_M = "Berlines Taille M",
    BERLINE_L = "Berlines Taille L",
    SUV = "SUV, Tout-terrains et Pick-up"
}

export enum StatusVehicule {
    REPARATION, HORS_SERVICES, EN_SERVICE
}

export enum Role {
    COLLABORATEUR, CHAUFFEUR, ADMINISTRATEUR
}

export class Vehicule {
    immatriculation:string;
    marque:string;
    modele:string;
    nbPlace:number;
    photo:string;
    categorie:Categorie;
    status:StatusVehicule;
    constructor(){
        
    }
}

export class Personne {
    nom:string;
    prenom:string;
    email:string;
    telephone:number;
    photo:string;
    matricule:string;
    password:string;
    role:Role;
    constructor(){
        
    }
}