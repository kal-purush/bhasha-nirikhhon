import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class BlogService {
  recentArticles;
 

  articles = [
    {
        "id": 1,
        "title":"Comment utiliser la directive ngStyle avec TypeScript ? ",
        "content":"<strong>Bonjour à tous !</strong> Vous vous demandez comment utiliser la directive ngStyle avec TypeScript ? Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?",
        "slug":"comment-utiliser-la-directive-ngstyle-avec-typescript",
        "author":"Roxane Anquetil",
        "publicationDate": "2020-07-24T09:00:00+00:00",
        "commentsNumber":3,
        "image": {
          "url": "/assets/img/formation_angular.png"
      },
        "training": {
              "category": {
                "name": "Développement web et mobile"
            },
            "skills": [
                {
                    "name": "Angular"
                },
                {
                    "name": "Développement Client"
                }
            ]
        },

        "comments": [
          {
            "id":1,
            "author":"Julien Durand",
            "publicationDate": "2020-09-24T09:00:00+00:00",
            "content":"Lorem Ipsum is simply dummy text of the pr make but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
            "website":"https://www.webstreet.io",
            "notation":2
          },
          {
            "id":2,
            "author":"Mohamed Bano",
            "publicationDate": "2020-08-29T09:00:00+00:00",
            "content":"Lorem Ipsum is simply dummy text of the pr make but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
            "website":"https://www.webstreet.io",
            "notation":3
          },
          {
            "id":3,
            "author":"Emma Rowling",
            "publicationDate": "2020-07-24T09:00:00+00:00",
            "content":"Lorem Ipsum is simply dummy text of the pr make but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
            "website":"https://www.webstreet-learning.com",
            "notation":4
          }

        ]

    },
    {
      "id": 2,
      "title":"C'est quoi un cookie informatique ?",
      "content":"Bonjour à tous ! Vous vous demandez ce qu'est un cookie informatique ? Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?",
      "slug":"cest-quoi-un-cookie-informatique",
      "author":"Roxane Anquetil",
      "publicationDate": "2020-12-24T09:00:00+00:00",
       "commentsNumber":2,
      "image": {
        "url": "/assets/img/formation_angular.png"
    },
      "training": {
            "category": {
              "name": "Développement web et mobile"
          },
          "skills": [
              {
                  "name": "Les bases du web"
              },
              {
                  "name": "Développement Client"
              }
          ]
      },
      "comments": [
        {
          "id":1,
          "author":"Julien Durand",
          "publicationDate": "2020-09-24T09:00:00+00:00",
          "content":"Lorem Ipsum is simply dummy text of the pr make but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
          "website":"",
          "notation":4
        },
        {
          "id":2,
          "author":"Mohamed Bano",
          "publicationDate": "2020-08-29T09:00:00+00:00",
          "content":"Lorem Ipsum is simply dummy text of the pr make but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
          "website":"",
          "notation":5
        }
 

      ]

  },
  {
    "id": 3,
    "title":"Comment centrer un bouton dans une division en CSS ? ",
    "content":"Bonjour à tous ! Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?",
    "slug":"comment-centrer-un-bouton-dans-une-division-en-css",
    "author":"Roxane Anquetil",
    "publicationDate": "2020-08-24T09:00:00+00:00",
     "commentsNumber":0,
    "image": {
      "url": "/assets/img/html-css-training.png"
  },
    "training": {
          "category": {
            "name": "Développement web et mobile"
        },
        "skills": [
            {
                "name": "Css"
            },
            {
                "name": "Html"
            }
        ]
    },
    "comments":[

    ]

}
  
];

getArticles() {
  return this.articles;
}

getArticleById(id:number) {
  const article = this.articles.find(
     (s) => {
       return s.id === id;
     }
   );
   return article;
}

getRecentArticles(id:number) {

  this.recentArticles = this.articles.sort((a, b) => {
    return <any>new Date(a.publicationDate) - <any>new Date(b.publicationDate);
  });

  return this.recentArticles;

}

getArticlesCategories() {
  var categories = [];

  this.articles.find(
    (s) => {
      categories.push(s.training.category.name);
    }
  );
 // categories.filter((item, index) => categories.indexOf(item) === index);
 categories = Array.from(new Set(categories));

  console.log(categories);
  return categories;
}



}