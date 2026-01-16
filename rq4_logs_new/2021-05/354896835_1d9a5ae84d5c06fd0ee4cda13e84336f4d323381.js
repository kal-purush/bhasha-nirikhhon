import About from './components/About';
import Home from './components/Home';
import HeaderLandingPage from './components/HeaderLandingPage';

export default {
    mode: 'history',
    routes: [
        {
            path: '/about',
            component: About
        },
        {
            path: '/',
            component: Home
        },
        {
            path: '/header-landing-page',
            component: HeaderLandingPage
        },
    ]
};