import { Injectable } from '@angular/core';
import { Category } from './../models/category.model';
import {Training} from './../models/training.model';
import { Module } from './../models/module.model';

@Injectable()

export class TrainingsService {
   

    trainings: Training[];


    getTrainings() {
        this.trainings = [
           {
            category:{id:1,name:"Développement Web"},
            title:"Angular - Créer des applications web",
            description: "Dans ce cours vous apprendrez à développer des applications web performantes avec Angular !",
            goals:["Développer des applications performantes avec Angular", "Maîtriser les clés du framework"],
            practicalWork: "Vous réaliserez une application Angular de A à Z!",
            pedagogicalMethod: "Développement web par la pratique et présentation de cas de développement issus du monde réel",
            durationInHours: 21,
            reference: "ANG",
            price: 1400,
            modules: 
            [
               {title:"Introduction à Angular", items:["Comprendre les version d'Angular", "Structure d'un projet Angular"]},
               {title:"Les fondamentaux de TypeScript", items:["TypeScript, c'est quoi ?", "Votre premier programme TypeScript"]},


            ]
              
           },
           {
            category:{id:3,name:"Design"},
            title:"WebDesign - Les fondamentaux",
            description: "Dans ce cours vous apprendrez les fondamentaux du web design.",
            goals:["Acquérir les bases du WebDesign", "Etre capable de créer des templates d'applications"],
            practicalWork: "Vous réaliserez différentes maquettes web avec le logiciel Adobe XD.",
            pedagogicalMethod:"Développement web par la pratique et présentation de cas de développement issus du monde réel",
            durationInHours: 21,
            reference: "WDE",
            price: 1400,
            modules: 
            [
               {title:"Introduction au WebDesign", items:["Concepts UX/UI", "Bibliothèques existantes"]},
               {title:"Les grilles Responsive", items:["Le responsive c'est quoi ?", "Votre première grille Responsive"]},


            ]
           }

          ];

          return this.trainings;
    }
}