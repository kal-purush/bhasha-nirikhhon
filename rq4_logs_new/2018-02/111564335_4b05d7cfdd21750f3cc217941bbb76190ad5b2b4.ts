import axios from 'axios';
import PersonRequest from '../types/services/PersonRequest';
import EngangsstonadSoknadRequest from '../types/services/EngangsstonadSoknadRequest';
const queryStringParser = require('query-string');

const defaultParams: PersonRequest = {
    fnr: '12341234',
    stub: true
};

const stub = () => ({
    data: {
        fornavn: 'Test',
        etternavn: 'Testesen'
    }
});

// tslint:disable-next-line no-any
declare const __ENV__: any;

function getPerson(params: PersonRequest = defaultParams) {
    if (__ENV__ === 'heroku') {
        return stub();
    }
    // tslint:disable-next-line no-any
    const endpoint = (<any> window).REST_API_URL;
    return axios.get(`${endpoint}/personinfo?${queryStringParser.stringify(params)}`);
}

function sendSoknad(soknad: EngangsstonadSoknadRequest) {
    // tslint:disable-next-line no-any
    return axios.post(`${(<any> window).REST_API_URL}/engangsstonad?stub=true`, soknad);
}

const Api = { getPerson, sendSoknad };

export default Api;