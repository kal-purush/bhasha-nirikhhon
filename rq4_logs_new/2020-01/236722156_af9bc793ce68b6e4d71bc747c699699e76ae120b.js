import * as axios from 'axios';

const instance = axios.create ({
    baseURL: 'http://app.mastercalc.com.ua/'

});

export const mastercalcAPI = {
    getInitialParams (param) {
        return instance.get(`get_transitions?cat=${param}&ini_req=initial`)
    },
    getRequiredParams (param) {
        return instance.get(`get_transitions?cat=${param}&ini_req=required`)
    },
    getEstimate (data) {
        return instance.post(`/get_estimate`)
    } 
}