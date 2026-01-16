import { maxLength, string } from '../rules';
import { validate } from '../validate';
import { hasElements } from './has-elements';

const rules = [string(), maxLength(10)];

describe('hasElements', () => {
  it('should pass null and undefined', () => {
    expect(validate(null, hasElements(rules)).passed()).toBe(true);
    expect(validate(undefined, hasElements(rules)).passed()).toBe(true);
  });

  it('should an array of results', () => {
    const values = [1, 2];
    const messages = validate(values, hasElements(rules)).all();
    expect(messages).toEqual(values.map(value => validate(value, rules).all()));
  });

  it('should validate each object individually', () => {
    const values = ['hello', 5, 'world'];
    const messages = validate(values, hasElements(rules)).all();
    expect(messages).toEqual(values.map(value => validate(value, rules).all()));
  });

  it('should work on all array like objects', () => {
    const string = 'hello';
    let messages = validate(string, hasElements(rules)).all();
    expect(messages).toEqual(Array.from(string).map(value => validate(value, rules).all()));

    const object = { 1: 'h', 2: 'i', length: 2 };
    messages = validate(object, hasElements(rules)).all();
    expect(messages).toEqual(Array.from(object).map(value => validate(value, rules).all()));
  });

  it('should correctly pass the key option', () => {
    const constraint = jest.fn(() => undefined);
    const parent = ['a', 'b', 'c'];
    validate(parent, hasElements([constraint]));
    parent.forEach((value, index) => {
      const options = expect.objectContaining({ key: index + '' });
      expect(constraint).nthCalledWith(index + 1, value, options);
    });
  });

  it('should correctly pass the key path option', () => {
    const constraint = jest.fn(() => undefined);
    const parent = ['a', 'b', 'c'];
    validate(parent, hasElements([constraint]));
    parent.forEach((value, index) => {
      const options = expect.objectContaining({ keyPath: [index + ''] });
      expect(constraint).nthCalledWith(index + 1, value, options);
    });
  });

  it('should correctly pass the key path option when nested', () => {
    const constraint = jest.fn(() => undefined);
    const child = ['a', 'b', 'c'];
    const parent = [child, child, child];
    validate(parent, hasElements(hasElements([constraint])));
    parent.forEach((_, parentIndex) => {
      child.forEach((value, childIndex) => {
        const index = parentIndex * child.length + childIndex;
        const options = expect.objectContaining({ keyPath: [parentIndex + '', childIndex + ''] });
        expect(constraint).nthCalledWith(index + 1, value, options);
      });
    });
  });

  it('should correctly pass the parent option', () => {
    const constraint = jest.fn(() => undefined);
    const parent = ['a', 'b', 'c'];
    validate(parent, hasElements([constraint]));
    parent.forEach((value, index) => {
      const options = expect.objectContaining({ parent });
      expect(constraint).nthCalledWith(index + 1, value, options);
    });
  });
});