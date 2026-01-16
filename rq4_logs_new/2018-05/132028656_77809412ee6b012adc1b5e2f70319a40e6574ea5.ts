import store from './store';

import {logoutUser} from './actions';

const apiFetch = (input: RequestInfo, init: RequestInit): Promise<any> => {

    init.headers = {'Content-Type': 'application/json'};
    init.credentials = 'include';

    return fetch(input, init).then(response => {

        const headers = response.headers.get('set-cookie');

        console.log(headers);

        if (!response.ok) {

            if (response.status === 401) {

                // todo can we move this ???
                localStorage.removeItem('authUser');

                store.dispatch(logoutUser());
            }

            throw new Error(response.statusText);
        }
        return response.json();
    });
};

export default apiFetch;