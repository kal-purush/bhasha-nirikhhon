let environment: string = 'dev';
let url: string;

if(environment == 'prod') {

    url = 'https://jsonplaceholder.typicode.com/users';

} else if(environment == 'dev') {

    url = 'https://jsonplaceholder.typicode.com/users';

}

export const API_URL = url;