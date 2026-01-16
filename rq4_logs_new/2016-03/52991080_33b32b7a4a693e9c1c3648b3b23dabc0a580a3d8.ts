import { IDispatcher } from "../../libs/store";

export class BaseActionCreator {
    constructor(public service,
        public dispatcher: IDispatcher,
        public guid,
        private addOrUpdateAction,
        private allAction,
        private removeAction
    ) { }

    getById = options => {
        var newId = this.guid();
        this.service.get().then(results => {
            var action = new this.addOrUpdateAction(newId, results);
            this.dispatcher.dispatch(action);
        });
        return newId;
    }

    all = () => {
        var newId = this.guid();
        this.service.get().then(results => {
            var action = new this.allAction(newId, results);
            this.dispatcher.dispatch(action);
        });
        return newId;
    }

    add = options => {
        var newId = this.guid();
        this.service.add({ data: options.data }).then(results => {
            var action = new this.addOrUpdateAction(newId, results);
            this.dispatcher.dispatch(action);
        });
        return newId;
    }

    remove = options => {
        var newId = this.guid();
        this.service.remove({
            id: options.entity.id
        }).then(results => {
            this.dispatcher.dispatch(new this.removeAction(newId, options.entity));
        });
        return newId;
    }
}

export class AddOrUpdateAuthorAction { constructor(public id, public entity) { } }

export class AllAuthorsAction { constructor(public id, public entities) { } }

export class RemoveAuthorAction { constructor(public id, public entityId) { } }

export class AuthorsFilterAction { constructor(public id, public term) { } }

export class SetCurrentAuthorAction { constructor(public id, public entityId) { } }