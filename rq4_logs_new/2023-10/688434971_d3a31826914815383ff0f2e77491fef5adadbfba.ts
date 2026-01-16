import {Injectable} from '@angular/core';
import {GoogleLoginProvider, SocialAuthService, SocialUser} from "@abacritt/angularx-social-login";
import {Router} from "@angular/router";
import {Observable, of, switchMap} from "rxjs";
import {takeUntilDestroyed} from "@angular/core/rxjs-interop";
import {HttpClient} from '@angular/common/http';
import {mapSocialUserToUserInterface, UserInterface} from "../../shared/model/user.interface";
import {environment} from "../../../environments/environment.development";

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  accessToken: string = '';
  user$: Observable<SocialUser> = this.authService.authState;

  constructor(private authService: SocialAuthService, private router: Router, private http: HttpClient) {}

  authenticateUser(user: SocialUser | null): Observable<UserInterface | null> {
    if (user === null){
      return of(null);
    }
    const userInstance = mapSocialUserToUserInterface(user);
    return this.http.post<UserInterface>(environment.localBackendBaseUrl + environment.authenticationUrl, userInstance);
  }

  getAccessToken(): void {
    this.authService.getAccessToken(GoogleLoginProvider.PROVIDER_ID).then(accessToken => this.accessToken = accessToken);
  }

  async refreshToken(): Promise<void> {
    return await this.authService.refreshAuthToken(GoogleLoginProvider.PROVIDER_ID);
  }

  signOut() {
    this.authService.signOut();
  }
}