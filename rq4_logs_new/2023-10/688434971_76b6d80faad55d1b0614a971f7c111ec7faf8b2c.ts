import {Injectable} from '@angular/core';
import {AuthService} from "../../../auth/service/auth.service";
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";
import {UserInterface} from "../../../auth/model/user.interface";
import {HttpClient} from '@angular/common/http';
import {environment} from "../../../../environments/environment.development";
import {take} from "rxjs";

@Injectable({
  providedIn: 'root',
})
export class FavoriteService {
  authenticatedUser: UserInterface | null = null;

  constructor(private authService: AuthService, private http: HttpClient) {
    this.authService.authenticatedUser.pipe(takeUntilDestroyed()).subscribe(user => {
      this.authenticatedUser = user;
    });
  }

  public addFavorite(ticker: string) {
    this.http.put<UserInterface>(environment.localBackendBaseUrl + environment.addfavoriteStockUrl + ticker, this.authenticatedUser)
      .pipe()
      .subscribe({
        next: data => {
          this.authService.authenticatedUser.next(data)
        },
        error: error => {
          console.error('There was an error!', error);
        }
      });
  }

  public removeFavorite(ticker: string) {
    this.http.put<UserInterface>(environment.localBackendBaseUrl + environment.removeFavoriteStockUrl + ticker, this.authenticatedUser)
      .pipe()
      .subscribe({
        next: data => {
          this.authService.authenticatedUser.next(data)
        },
        error: error => {
          console.error('There was an error!', error);
        }
      });
  }
}