import React from 'react';
import PropTypes from 'prop-types';
import { Switch, Route, matchPath, useLocation } from 'react-router-dom';
import Album from './pages/Album';
import Favorites from './pages/Favorites';
import Home from './pages/Home';
import Profile from './pages/Profile';
import ProfileEdit from './pages/ProfileEdit';
import Footer from './components/Footer';
import Header from './components/Header';
import NotFound from './pages/NotFound';

export const ROUTES = [
  {
    path: '/album/:id',
    component: Album,
    exact: true,
  },
  {
    path: '/search',
    component: Home,
    exact: true,
  },
  {
    path: '/favorites',
    component: Favorites,
    exact: true,
  },
  {
    path: '/profile/edit',
    component: ProfileEdit,
    exact: true,
  },
  {
    path: '/profile',
    component: Profile,
    exact: true,
  },
];

export const RenderPrivateRoutes = ({ routes }) => {
  const location = useLocation();
  const exists = routes.some((route) => matchPath(location.pathname, {
    path: route.path,
    exact: true,
    strict: false,
  }));

  if (!exists) return <Route component={ NotFound } />;

  return (
    <>
      <Header />
      <Switch>
        {routes.map((route, i) => (
          <Route
            key={ i }
            path={ route.path }
            exact={ route.exact }
            render={ (props) => (
              <route.component { ...props } />
            ) }
          />
        ))}
      </Switch>
      <Footer />
    </>
  );
};

RenderPrivateRoutes.propTypes = {
  routes: PropTypes.arrayOf(PropTypes.shape({
    path: PropTypes.string,
    component: PropTypes.func,
    exact: PropTypes.bool,
  })).isRequired,
};