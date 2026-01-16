export interface IShows
{
    Id: number,
    original_title: string,
    vote_average: number,
    release_date: string,
    overview: string,
    Year: string,
    poster_path: string
}



/* 
    <tbody>
        <!-- *ngFor="let hero of heroes"
        *ngIf='filteredShows != null' -->

    <tr *ngFor="let show of shows">
      <!-- put in a href -->
      <td><img class='Poster' [src]= "getUrl(show.poster_path)"></td>
      <td>{{show.original_title}}</td>
      <td>{{show.Type}}</td>
      <td>{{show.release_date}}</td>
      <td>{{show.overview}}</td> 
    </tr>
  </tbody>
*/