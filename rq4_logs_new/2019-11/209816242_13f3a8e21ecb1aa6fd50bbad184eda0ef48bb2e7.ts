/**
 * @author Hadi Horani, s144885
 * @author Anders Wiberg Olsen, s165241
 */

import { State, Action, StateContext, Selector } from '@ngxs/store';
import { Login } from '../_actions/token.actions';
import { UserService } from '../_services/login.service';
import { User } from '../models/login/user.model';
import { tap } from 'rxjs/operators';
import { AuthService } from '../_services/auth.service';

export class TokenStateModel {
    token?: string;
    user: User;
}

@State<TokenStateModel>({
    name: 'token'
})
export class TokenState {
    constructor(private userService: UserService, private authService: AuthService) {}
    
    @Selector()
    static getToken(state: TokenStateModel) {
        return state.token;
    }

    @Selector()
    static getUser(state: TokenStateModel) {
        return state.user;
    }

    @Action(Login)
    login({ patchState }: StateContext<TokenStateModel>) {
        const token = localStorage.getItem('token');
        if (token !== null) {
            patchState({ token });
            return this.userService.fetchUser().pipe(tap((user: User) => {
                patchState({ user });
            }));
        } else {
            this.authService.loginWithSso();
        }
    }
}