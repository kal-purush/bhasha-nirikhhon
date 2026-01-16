import { IDispatcher } from "../../libs/store";

export interface IAuthorActionCreator {
    get(): string;
    addAuthor(options: any): string;
}

export class AuthorActionCreator implements IAuthorActionCreator {
    constructor(private authorService, private dispatcher: IDispatcher, private guid) { }

    get = () => {
        var newId = this.guid();
        this.authorService.get().then(results => {
            var action = new AllAuthorsAction(newId, results);
            this.dispatcher.dispatch(action);
        });
        return newId;
    }

    addAuthor = options => {
        var newId = this.guid();
        this.authorService.add({ data: options.data }).then(results => {
            var action = new AddOrUpdateAuthorAction(newId, results);
            this.dispatcher.dispatch(action);
        });
        return newId;
    }

}

export class AddOrUpdateAuthorAction { constructor(public id, public entity) { } }

export class AllAuthorsAction { constructor(public id, public entities) { } }

export class RemoveAuthorAction { constructor(public id, public entityId) { } }

export class AuthorsFilterAction { constructor(public id, public term) { } }

export class SetCurrentAuthorAction { constructor(public id, public entityId) { } }