import Repository from "./repository";
export * from "./repository"
let Octokat = require('octokat')

export class GitManager {
    octo: any

    constructor(token) {
        this.octo = new Octokat({ token: token });
    }

    repo(owner, name) {
        return new Repository(this.octo.repos(owner, name), owner, name);
    }
}