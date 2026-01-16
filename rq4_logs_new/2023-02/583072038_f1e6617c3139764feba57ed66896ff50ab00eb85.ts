import Block from '~/src/utils/block';
import { FormProps } from '~/src/utils/prop-types';
import { AuthController } from '~/src/controllers/auth-controller';
import { ChatsController } from '~/src/controllers/chats-controller';
import { SettingsController } from '~/src/controllers/settings-controller';

import template from './form-avatar.hbs';

export default class FormAvatar extends Block {
    constructor(props: FormProps) {
        const auth = new AuthController();
        const settings = new SettingsController();
        const chats = new ChatsController();

        props.events = {
            async submit(e) {
                try {
                    e.preventDefault();
                    console.log('uploading avatar');
                } catch (error: any) {
                    alert(`Oops, something went wrong: ${error.message}`);
                    console.error(error.message);
                }
            },
        };

        super(props, 'form');

        if (!this.element) {
            return;
        }

        this.element.setAttribute('id', props.id);
        this.element.setAttribute('action', '#');
    }

    render() {
        return this.compile(template, { ...this.props });
    }
}