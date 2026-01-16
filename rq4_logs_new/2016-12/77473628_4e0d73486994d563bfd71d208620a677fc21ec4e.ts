import {EntityStore} from './entity-store.model';
import {Entity} from './entity.model';

describe('EntityStore', () => {
  let store: EntityStore;

  describe('.constrctor', () => {
    describe('with no entity list', () => {

      beforeEach(() => {
        store = new EntityStore();
      });

      it('should have no entities', () => {
        expect(store.all()).toEqual([]);
      });
    });

    describe('with an entity list', () => {
      let originalEntities, clonedEntities;

      beforeEach(() => {
        originalEntities = [
          jasmine.createSpyObj('Original Entity 1', ['clone']),
          jasmine.createSpyObj('Original Entity 2', ['clone'])
        ];

        clonedEntities = [
          jasmine.createSpyObj('Cloned Entity 1', ['clone']),
          jasmine.createSpyObj('Cloned Entity 2', ['clone'])
        ];

        originalEntities[0].clone.and.returnValue(clonedEntities[0]);
        originalEntities[1].clone.and.returnValue(clonedEntities[1]);

        clonedEntities[0].clone.and.returnValue(1);
        clonedEntities[1].clone.and.returnValue(2);

        store = new EntityStore(originalEntities);
      });

      it('should store the cloned entitites', () => {
        expect(store.all()).toEqual([1, 2]);
      });
    });
  });

  describe('.get', () => {
    describe('with no existing entity id', () => {

      beforeEach(() => {
        store = new EntityStore();
      });

      it('should return null', () => {
        expect(store.get(1)).toBeNull();
      });
    });

    describe('with an existing entity id', () => {
      let originalEntity, clonedEntity, returnedEntity;

      beforeEach(() => {
        returnedEntity = {};
        originalEntity = jasmine.createSpyObj('Original Entity', ['clone']);
        clonedEntity = jasmine.createSpyObj('Original Entity', ['clone']);

        originalEntity.clone.and.returnValue(clonedEntity);
        clonedEntity.id = 1;
        clonedEntity.clone.and.returnValue(returnedEntity);

        store = new EntityStore([originalEntity]);
      });

      it('should return a clone of the entity', () => {
        expect(store.get(1)).toBe(returnedEntity);
      });
    });
  });

  describe('.remove', () => {
    let result, entity;

    beforeEach(() => {
      entity = jasmine.createSpyObj('Entity', ['clone']);
      entity.id = 1;
      entity.clone.and.returnValue(entity);
      store = new EntityStore([entity]);
      result = store.remove(entity);
    });

    it('should not modify the original entity store', () => {
      expect(store.all()).toEqual([entity]);
    });

    it('should return an EntityStore without an entity with matching id', () => {
      expect(result.all()).toEqual([]);
    });
  });

  describe('.store', () => {
    let stored: EntityStore;
    let originalEntity, clonedEntity, twiceClonedEntity, expectedResult, storedEntity, clonedStored, thriceClonedEntity;

    beforeEach(() => {
      originalEntity = jasmine.createSpyObj('Entity', ['clone']);
      originalEntity.id = 0;

      clonedEntity = jasmine.createSpyObj('Entity', ['clone']);
      clonedEntity.id = 1;

      twiceClonedEntity = jasmine.createSpyObj('Entity', ['clone']);
      twiceClonedEntity.id = 2;

      thriceClonedEntity = jasmine.createSpyObj('Entity', ['clone']);
      thriceClonedEntity.id = 3;

      expectedResult = {};
      originalEntity.clone.and.returnValue(clonedEntity);
      clonedEntity.clone.and.returnValue(twiceClonedEntity);
      twiceClonedEntity.clone.and.returnValue(thriceClonedEntity);

      clonedStored = jasmine.createSpyObj('Entity', ['clone']);
      clonedStored.id = 5;
      storedEntity = jasmine.createSpyObj('Entity', ['clone']);
      storedEntity.clone.and.returnValue(clonedStored);
      clonedStored.clone.and.returnValue(expectedResult);

      store = new EntityStore([originalEntity]);
    });

    describe('with the entity already in the store', () => {

      beforeEach(() => {
        storedEntity.id = 2;
        stored = store.store(storedEntity);
      });

      it('should not modify the original store', () => {
        expect(store.all().length).toBe(1);
        expect(store.get(clonedEntity.id)).toBe(twiceClonedEntity);
      });

      it('should replace the entity in the store', () => {
        expect(stored.all().length).toBe(1);
        expect(stored.get(5)).toBe(expectedResult);
      });
    });

    describe('with the entity not in the store', () => {

      beforeEach(() => {
        storedEntity.id = 20;
        stored = store.store(storedEntity);
      });

      it('should not modify the original store', () => {
        expect(store.all().length).toBe(1);
        expect(store.get(clonedEntity.id)).toBe(twiceClonedEntity);
      });

      it('should add the entity to the store', () => {
        expect(stored.all().length).toBe(2);
        expect(store.get(clonedEntity.id)).toBe(twiceClonedEntity);
        expect(stored.get(5)).toBe(expectedResult);
      });
    });
  });

});