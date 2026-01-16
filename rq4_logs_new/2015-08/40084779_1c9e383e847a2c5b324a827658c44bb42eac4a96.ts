module Extropy.Entity {

    export class BaseEntityManager {
        private _lastId = 0;

        protected _trackedEntities: EntityImplementation[] = [];
        private _idToEntity = {};
        private _componentManagers = [];

        public get element(): HTMLElement {
            return null;
        }

        public get unscaledElement(): HTMLElement {
            return null;
        }

        public createEntity(name?: string, id?: string): Entity {

            var idToAssign = id || this._lastId.toString();

            var entity = this._createEntity(idToAssign);
            this._lastId++;
            this._trackedEntities.push(entity);
            this._idToEntity[entity.id] = entity;
            entity.name = name;
            return entity;
        }

        public deleteEntity(entity: Entity) {
            var idx = this._trackedEntities.indexOf(<EntityImplementation>entity);
            if (idx > -1) {

                var entImpl = <EntityImplementation>this._trackedEntities[idx];
                entImpl._onDestroy();

                this._trackedEntities.splice(idx, 1);
                delete this._idToEntity[entity.id];
            }
        }

        public getEntities(filterFunc?: (ent: Entity) => boolean) {
            var entitiesToReturn = [];

            for (var i = 0; i < this._trackedEntities.length; i++) {
                if (!filterFunc || filterFunc(this._trackedEntities[i]))
                    entitiesToReturn.push(this._trackedEntities[i]);
            }

            return entitiesToReturn;
        }

        public getEntityByName(name: string): Entity {
            for (var i = 0; i < this._trackedEntities.length; i++) {
                if (this._trackedEntities[i].name === name)
                    return this._trackedEntities[i];
            }
        }

        protected /*virtual*/ _createEntity(id: string): EntityImplementation {
            return new EntityImplementation(id, this);
        }

        public getEntityById(id: string): Entity {
            return this._idToEntity[id];
        }

        public update() {
            this._trackedEntities.forEach((val) => {
                //Log.verbose(LogArea.EntityManager, "[BEGIN] - Update - " + val.name);
                val.update();
                //Log.verbose(LogArea.EntityManager, "[END] - Update - " + val.name);
            });

            this._trackedEntities.forEach((val) => {
                //Log.verbose(LogArea.EntityManager, "[BEGIN] - PostUpdate - " + val.name);
                val.postUpdate();
                //Log.verbose(LogArea.EntityManager, "[END] - PostUpdate - " + val.name);
            });
        }

        public render() {
            this._trackedEntities.forEach((val) => {
                //Log.verbose(LogArea.EntityManager, "[BEGIN] - Render - " + val.name);
                val.render();
                //Log.verbose(LogArea.EntityManager, "[END] - Render - " + val.name);
            });
        }
    }
}
