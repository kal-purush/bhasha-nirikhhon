import Block from '~/src/utils/block';
import Button from '~/src/components/button';
import { AuthController } from '~/src/controllers/auth-controller';
import template from './logout.hbs';

export default class PageLogout extends Block {
    constructor() {
        super({}, 'div');

        this.element.classList.add(
            'window',
            'lg',
            'p-2/5',
            'auth',
            'w-fixed',
            'signin',
            'bg-orange'
        );
    }

    init() {
        const auth = new AuthController();
        this.children.buttonLogout = new Button({
            title: 'Log Out',
            id: 'logout',
            css: ['bg-orange'],
            link: '',
            events: {
                async click(e) {
                    e.preventDefault();
                    await auth.logout();
                },
            },
        });
    }

    render() {
        this.dispatchComponentDidMount();

        return this.compile(template, {
            ...this.props,
        });
    }
}