export interface api {
    url: string,
    name: string,
    type: Endpoint
}

export enum Endpoint {
    JOKE = 0,
    PROGRAMMING = 1
}