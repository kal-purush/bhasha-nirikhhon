import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class SessionTopFiveService {

 

 sessionTopFives = [
   {
     'cities': [
       {
         "id":1,
         "name":"Paris",
         "slug":"paris"
       },
       {
        "id":2,
        "name":"Rouen",
        "slug":"rouen"
      },
      {
        "id":4,
        "name":"Marseille",
        "slug":"marseille"
      },
      {
        "id":5,
        "name":"Lyon",
        "slug":"lyon"
      },
      {
        "id":8,
        "name":"Toulouse",
        "slug":"toulouse"
      }
    ],
     'subCategories': [
       {
        "id":3,
        "name":"Bureautique",
        "slug":"bureautique"
       },
       {
        "id":4,
        "name":"Développement web",
        "slug":"developpement-web"
       },
       {
        "id":10,
        "name":"Management",
        "slug":"management"
       },
       {
        "id":2,
        "name":"Gestion des projets et des SI",
        "slug":"gestion-des-projets-et-des-si"
       },
       {
        "id":15,
        "name":"Anglais",
        "slug":"anglais"
       }
      ],

     'skills': [
         {
        "id":6,
        "name":"Excel",
        "slug":"excel"
       },
       {
         "id":9,
         "name":"Agilité",
         "slug":"agilite"
       },
       {
         "id":9,
         "name":"Développement web",
         "slug":"developpement-web"
       },
       {
         "id":9,
         "name":"Coaching",
         "slug":"coaching"
       },
       {
         "id":9,
         "name":"Php",
         "slug":"php"
       }

    ]
   }

 ];

 getSessionTopFives() {
    return this.sessionTopFives;
}
}