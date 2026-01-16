import Block from '~/src/utils/block';
import ButtonIcon from '~/src/components/button-icon';
import template from './pop-up-chat-actions.hbs';

export default class PopUpChatActions extends Block {
    constructor() {
        super({}, 'div');
    }

    init() {
        this.children.buttonBack = new ButtonIcon({
            title: 'Close',
            id: 'close',
            icon: 'close',
            css: ['bg-orange'],
            action: 'close',
            events: {
                click(e) {
                    e.preventDefault();
                    // close pop-up
                    const popUpWrap = document.querySelector('.pop-up');
                    if (popUpWrap) {
                        popUpWrap.remove();
                    }
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