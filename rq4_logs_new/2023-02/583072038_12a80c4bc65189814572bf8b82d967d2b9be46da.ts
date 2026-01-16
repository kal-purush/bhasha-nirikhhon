import Block from '~/src/utils/block';
import ButtonIcon from '~/src/components/button-icon';
import template from './pop-up-add-chat.hbs';

export default class PopUpAddChat extends Block {
    constructor({}) {
        super({}, 'div');
        this.element.classList.add('window', 'w-fixed', 'lg', 'account', 'bg-cyan');
    }

    init() {
        this.children.buttonBack = new ButtonIcon({
            title: 'Close',
            id: 'close',
            icon: 'back',
            css: ['bg-orange'],
            action: 'close',
            events: {
                click(e) {
                    e.preventDefault();
                    console.log('close pop-up');
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