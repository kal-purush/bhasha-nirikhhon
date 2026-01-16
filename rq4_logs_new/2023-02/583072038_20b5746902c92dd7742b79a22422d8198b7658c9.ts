import { MessagesAPI } from '../api/messages-api';
import store from '~/src/utils/store';

export class MessagesController {
    #api = new MessagesAPI();

    async connect() {
        try {
            const data = this.#api.connect();
            console.log(data);
            return data;
        } catch (e: any) {
            alert(`Oops, something went wrong: ${e.message}`);
            console.error(e.message);
        }
    }

    async loadMessages() {
        try {
            /** @todo replace with state management to avoid oldschool frontend override */
            const messagesWrap = document.querySelector('.messages');
            if (!messagesWrap) {
                return;
            }
            messagesWrap.innerHTML = '';
            await this.connect();
        } catch (e: any) {
            alert(`Oops, something went wrong: ${e.message}`);
            console.error(e.message);
        }
    }
}