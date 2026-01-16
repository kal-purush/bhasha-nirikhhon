/**
 * @jest-environment jsdom
 */
// @ts-nocheck

import Block from '~/src/utils/block';
import store from '~/src/utils/store';
// import { AuthController } from '~/src/controllers/auth-controller';
// import { ChatsController } from '~/src/controllers/chats-controller';
// import { MessagesController } from '~/src/controllers/messages-controller';

export default class Router {
    areWeTestingWithJest() {
        return process.env.JEST_WORKER_ID !== undefined;
    }

    routesData;

    #baseUrl = new URL(window.location.href).origin;

    #history;

    constructor(routesData: Record<string, Block>) {
        this.routesData = routesData;
        this.#history = window.history;
    }

    async init() {
        // check if user is logged in on page reload
        const isLoggedIn = await this.#isLoggedIn();
        if (isLoggedIn) {
            // fetch chats
            await this.#getChats();
        }
        const path = this.getPath(window.location.href);
        this.load(path);
        // load template on history change
        window.onpopstate = (event: PopStateEvent) => {
            const target = event.currentTarget as Window;
            this.load(target.location?.pathname.substring(1), true);
        };
        this.addLinksClickListener();
    }

    // Output respective template on page and optionally update history
    async load(path: string, skipHistoryUpdate?: boolean) {
        const pathReal = this.#withAuth(path);
        const template = this.getTemplate(pathReal);
        const root = document.getElementById('root');
        if (root) {
            root.innerHTML = '';
            root.append(template.component?.getContent());
        }
        this.updateBgColor(template.name);
        if (!skipHistoryUpdate) {
            window.history.pushState({ pathReal }, '', `${this.#baseUrl}/${pathReal}`);
        }
        // close ws connection on page change/reload
        const messages = new MessagesController();
        await messages.disconnect();
    }

    back() {
        this.#history.back();
    }

    forward() {
        this.#history.forward();
    }

    // Check if user is logged in
    async #isLoggedIn() {
        const authC = new AuthController();
        const user = await authC.getUser();
        return user;
    }

    // Get chats
    async #getChats() {
        const chatsC = new ChatsController();
        // const chats = await chatsC.request();
        // return chats;
        return 123;
    }

    // Apply auth state to provided path
    #withAuth(path: string) {
        const pagesPublic = ['', 'sign-up'];
        const pagesPrivate = ['settings', 'messenger', 'logout'];
        if (store.getState()?.user && pagesPublic.includes(path)) {
            return 'logout';
        }
        if (!store.getState()?.user && pagesPrivate.includes(path)) {
            return '';
        }
        return path;
    }

    // Get template data
    getTemplate(path: string) {
        const pathData = this.getPathData(path);
        const template = pathData.path ? pathData.path : 'sign-in';
        return !this.routesData[template]
            ? { name: 'page-404', component: this.routesData['page-404'] }
            : { name: template, component: this.routesData[template] };
    }

    // Add temporary nav link click event listeners for Sprint #1-2
    addLinksClickListener() {
        // @todo rafactor as needed
        // @todo add support for browser history changes with window.onpopstate - will do in Sprint #3
        const buttonLinks = document.querySelectorAll('.nav-link');
        buttonLinks.forEach(el => {
            el.addEventListener('click', e => {
                e.preventDefault();
                if (!(e.currentTarget instanceof HTMLElement)) {
                    return;
                }
                const isHome =
                    !e.currentTarget.dataset.path || e.currentTarget.dataset.path === '/';
                const linkPath =
                    isHome || !e.currentTarget.dataset.path ? '' : e.currentTarget.dataset.path;
                this.load(linkPath);
            });
        });
        // add header test router back/forward links
        const routerLinkBack = document.querySelector('.router-link.back');
        routerLinkBack?.addEventListener('click', e => {
            e.preventDefault();
            this.back();
        });
        const routerLinkForward = document.querySelector('.router-link.forward');
        routerLinkForward?.addEventListener('click', e => {
            e.preventDefault();
            this.forward();
        });
    }

    // Little helper to get path from provided location.href
    getPath(href: string) {
        const url = new URL(href);
        return `${url.pathname ? url.pathname.replace('/', '') : ''}`;
    }

    // Little helper to get naked path from provided full path
    getPathData(path: string) {
        const url = new URL(`${this.#baseUrl}/${path}`);
        return {
            path: url.pathname.replace('/', ''),
        };
    }

    // Little helper to update body background
    updateBgColor(templateName: string) {
        const { body } = document;
        let color = 'purple';
        if (templateName === 'signUp') {
            color = 'cyan';
        }
        if (templateName === 'messenger') {
            color = 'orange';
        }
        if (templateName === 'page-404') {
            color = 'pink';
        }
        if (templateName === 'page-500') {
            color = 'red';
        }
        body.dataset.bgColor = color;
    }
}