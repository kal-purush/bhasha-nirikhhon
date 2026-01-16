import VueRouter from 'vue-router';

let routes = [
    {
        path: '/',
        component: require('./views/Create')
    },
    {
        name: 'show',
        path: '/show',
        component: require( './views/Show')
    },
    {
        name: 'view',
        path: '/users/:user_id',
        component: require('./views/Show')
    }
];

export default new VueRouter({
    routes,
    linkActiveClass: 'nav-link active'
});