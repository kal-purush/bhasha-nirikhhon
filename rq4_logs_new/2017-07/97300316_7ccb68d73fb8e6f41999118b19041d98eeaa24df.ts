import { ObjectCleaner } from './ObjectCleaner';

describe('ObjectCleaner', () => {
  it(`should fill missing fields in objects according to reference object
    if they are simple type`, () => {
    const referenceObject: Object = {
      a: 'a', b: 'b', c: 'c'
    };

    const objectsToFill: Array<Object> = [
      { c: 'c' },
      { a: 'a', b: 'b' },
      {}
    ];

    const filledObjects = ObjectCleaner.fillMissingFields(referenceObject, objectsToFill);

    expect(filledObjects.length).toBe(objectsToFill.length);

    filledObjects.forEach(filledObj => {
      Object.keys(referenceObject)
        .forEach(refObjKey => {
          expect(filledObj.hasOwnProperty(refObjKey)).toBeTruthy();
          expect(filledObj[refObjKey]).toBe(referenceObject[refObjKey]);
        });
    });
  });
});