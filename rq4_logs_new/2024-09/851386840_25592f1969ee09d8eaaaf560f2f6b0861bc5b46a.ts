import {Injectable} from '@angular/core';
import {Observable} from "rxjs";
import {Peliculas} from "../models/peliculas";


@Injectable({
  providedIn: 'root'
})
export class WatchlistService {
  getWatchlist(): Observable<Peliculas[]> {
    return new Observable((resolve) => {
      let watchlist = localStorage.getItem("Watchlist") ?? "";
      if (watchlist == "") {
        resolve.next([]);
        return;
      }
      resolve.next(JSON.parse(watchlist))
    })
  }

  addWatchlist(pelicula: Peliculas) {
    this.getWatchlist().subscribe((watchlist: Peliculas[]) => {
      let peliculaEncontrada = watchlist.filter(x => x.Id == pelicula.Id);
      if (peliculaEncontrada.length == 0) {
        watchlist.push(pelicula);
        localStorage.setItem("Watchlist", JSON.stringify(watchlist))
      }
    })
  }

  eliminateWatchlist(pelicula: Peliculas) {
    this.getWatchlist().subscribe((watchlist: Peliculas[]) => {
      let peliculaEliminada = watchlist.findIndex(x => x.Id == pelicula.Id);
      console.log("pelicula a eliminar",peliculaEliminada);
      if (peliculaEliminada >= 0) {
        watchlist.splice(peliculaEliminada, 1)
        localStorage.setItem("Watchlist", JSON.stringify(watchlist))
      }
    })
  }

  buscarWatchlist(pelicula:Peliculas):Observable<boolean>{
    return new Observable((resolve) => {
      this.getWatchlist().subscribe((watchlist: Peliculas[]) => {
        let peliculabuscar = watchlist.findIndex(x => x.Id == pelicula.Id);
        if (peliculabuscar >= 0) {
          resolve.next(true);
          return;
        }
        resolve.next(false);
        return;
      })
    })
  }

  constructor() {
  }
}