import {RouterStateSerializer} from '@ngrx/router-store';
import {RouterStateSnapshot} from '@angular/router';
import {RouterState} from '../state/router';

export class CustomRouterStateSerializer implements RouterStateSerializer<RouterState> {
  serialize(routerState: RouterStateSnapshot): RouterState {
    let route = routerState.root;
    while (route.firstChild) {
      route = route.firstChild;
    }

    const {url} = routerState;
    const queryParams = routerState.root.queryParams;
    const params = route.params;

    return {url, params, queryParams};
  }
}