import {createRouter, createWebHistory} from 'vue-router'
import Home from "@/views/Home";
import About from '../views/About'

const DEFAULT_TITLE = 'Untitled';
const SUFFIX = ' | Home Project'
const routes = [
    {
        path: '/',
        name: 'Home',
        component: Home,
        meta: {
            title: 'Home',
        }
    },
    {
        path: '/about',
        name: 'About',
        component: About,
        meta: {
            title: 'About',
        },
    }
]

const router = createRouter({
    history: createWebHistory(process.env.BASE_URL),
    routes,
})

router.beforeEach((to, from, next) => {
    document.title = (to.meta.title || DEFAULT_TITLE) + SUFFIX;
    next();
})

export default router