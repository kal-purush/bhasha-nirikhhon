import React from 'react';
import { Switch, Redirect } from 'react-router-dom';
import {
  Home as HomeView,
  Login as LoginView,
  Leaderboard as LeaderboardView,
} from './views';
import { RouteWithLayout } from './components';
import { Main as MainLayout } from './layouts';

// const Routes = () => {
//   return (
//     <div>
//     <Route exact path="/" component={HomeView}/>
//     <Route path="/login" component={LoginView}/>
//     <Route path="/leaderboard" component={LeaderboardView}/>
//     </div>
//   )
// };

const Routes = () => {
  return (
    <Switch>
      <RouteWithLayout
        component={HomeView}
        exact
        layout={MainLayout}
        path="/"
      />
      <RouteWithLayout
        component={LoginView}
        exact
        layout={MainLayout}
        path="/login"
      />
    </Switch>
  )
}

export default Routes;