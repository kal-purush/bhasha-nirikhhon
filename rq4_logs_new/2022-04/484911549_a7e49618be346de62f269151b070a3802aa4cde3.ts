import axios, { AxiosInstance } from 'axios';
import logging from '../utils/logging';
import { gql, GraphQLClient, request } from 'graphql-request';

const NAMESPACE: string = "ERAPI";

interface Item {
    id: string,
    name: string,
    image: string,
    description: string,
    type: string,
    effect: string
}

export default class ERApiController {

    private api: AxiosInstance = axios.create({
        baseURL: 'https://eldenring.fanapis.com/api/'
    });

    public async searchItem(name: string) {
        let query = gql`
        query get {
            item(search: ${name}) {
                id,
                name,
                image,
                description,
                type,
                effect
            }
        }`;

        const graphQLClient = new GraphQLClient('https://eldenring.fanapis.com/api/graphql');
        const result = await graphQLClient.request(query);
        return result;
    }

    public async getItemById(id: string) {
        let query = gql`
        query get {
            item(id: "${id}") {
                name,
                image,
                description,
                type,
                effect
            }
        }`;

        const graphQLClient = new GraphQLClient('https://eldenring.fanapis.com/api/graphql');
        const result = await graphQLClient.request(query);
        return result;
    }
}