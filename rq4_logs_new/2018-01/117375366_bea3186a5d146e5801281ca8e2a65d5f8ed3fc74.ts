import { LocalStorageUtilities } from './local-storage.utilities';

fdescribe('Static LocalStorageUtilities', () => {

  describe('#storageAvailableCheck', () => {
    it ('should be defined', () => {
      expect(LocalStorageUtilities.storageAvailableCheck).toBeDefined();
    });
  });

  describe('#objectToString', () => {
    it('should be defined', () => {
      expect(LocalStorageUtilities.objectToString).toBeDefined();
    });

    it('should convert an object to a string', () => {
      expect(LocalStorageUtilities.objectToString(testObject)).toEqual(testObjectString);
    });
  });

  describe('#stringToObject', () => {
    it('should be defined', () => {
      expect(LocalStorageUtilities.stringToObject).toBeDefined();
    });

    it('should convert a string to an object', () => {
      expect(LocalStorageUtilities.stringToObject(testObjectString)).toEqual(testObject);
    });
  });

  describe('#stringToTypedObject', () => {
    it('should be defined', () => {
      expect(LocalStorageUtilities.stringToTypedObject).toBeDefined();
    });

    it('should convert a string to an object', () => {
      const typedString = LocalStorageUtilities.stringToTypedObject<TestObject>(testObjectString);

      expect(typedString).toEqual(testObject);
    });
  });

});


// Test Mock Data, Etc...
////////////////////////////////////////////////
export class TestObject {
  name: string;
  age: number;
}

export const testObject: TestObject = {
  name: 'bob',
  age: 33
};

export const testObjectString = '{"name":"bob","age":33}';