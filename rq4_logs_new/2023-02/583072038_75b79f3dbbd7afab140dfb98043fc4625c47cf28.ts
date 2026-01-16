import Block from '~/src/utils/block';
import { ButtonProps } from '~/src/utils/prop-types';
import template from './button-avatar.hbs';

export default class ButtonAvatar extends Block {
    constructor(props: ButtonProps) {
        super(props, 'button');

        if (!this.element) {
            return;
        }

        // add default classes
        this.element.classList.add('button', 'w-100', 'h-100');
        this.element.setAttribute('id', props.id);

        // keep button as type="button" to avoid form submission
        if (props.link || props.action) {
            this.element.setAttribute('type', 'button');
        } else {
            this.element.setAttribute('type', 'submit');
        }
    }

    render() {
        return this.compile(template, {});
    }
}