export function mockRepository<T extends { id: number }>(domain: any) {
    const entities: Array<T> = [];

    return {
        reset: () => {
            entities.splice(0, entities.length);
        },
        initialize: (count: number) => {
            for (let i = 0; i < count; i++) {
                entities.push({ id: i + 1 } as T);
            }
        },
        save: (entity) => {
            entity.id = entities.length + 1;
            entities.push(entity);
            return entity;
        },
        find: () => {
            return entities;
        },
        findOne: (id) => {
            return entities[id - 1];
        },
        delete: (id) => {
            delete entities[id - 1];
        },
        update: (id, update) => {
            entities[id - 1] = Object.assign({}, entities[id - 1], update);
        },
    };
}