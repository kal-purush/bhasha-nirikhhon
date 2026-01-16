import { ChatsAPI } from '../api/chats-api';
import { UserProps } from '~/src/utils/prop-types';
import store from '~/src/utils/store';

export class ChatsController {
    #api = new ChatsAPI();

    async request() {
        try {
            const response = (await this.#api.request()) as XMLHttpRequest;
            /** @todo add common function */
            const responseText = JSON.parse(response.response);
            if (response.status !== 200) {
                const { reason } = responseText;
                console.warn(`Oops, something went wrong: ${reason}`);
                alert(`Oops, something went wrong: ${reason}`);
                return;
            }
            /** @todo refactor to centralized update */
            store.set('chats', responseText);
        } catch (e: any) {
            alert(`Oops, something went wrong: ${e.message}`);
            console.error(e.message);
        }
    }
}