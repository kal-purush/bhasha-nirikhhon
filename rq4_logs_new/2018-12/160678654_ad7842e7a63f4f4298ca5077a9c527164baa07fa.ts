import { Injectable } from '@angular/core';

@Injectable({
  providedIn: "root"
})
export class PostsDataService {

  private posts : any[] = [
    {id: 1, title: 'Mon premier post', content: 'Voilà mon premier article'},
    { id: 2, title: 'Mon deuxième post', content: 'Et mon deuxième article'},
    { id: 3, title: 'Mon troisième post', content: 'Maintenant mon troisième article'},
    { id: 4, title: 'Mon quatrième post', content: 'Piur finir mon quatrième article'},
  ];

  constructor() {}

  fetchData(){
    return this.posts;
  };
}