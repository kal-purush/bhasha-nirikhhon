import { Subject } from 'rxjs';
import createContextStore, { ContextStore, ContextStoreBase } from './create-context-store';

jest.mock('rxjs');

describe('createContextStore', () => {
  const contextStore = createContextStore({
    data: {
      test: '1234',
    },
    methods: {
      setTest(test: string): void {
        this.data.test = test;
      },
    },
  });

  it('should be a function', () => {
    expect(createContextStore).toBeInstanceOf(Function);
  });

  it('should return an object', () => {
    expect(contextStore).toBeInstanceOf(Object);
  });

  describe('data', () => {
    it('should have a data property', () => {
      expect(contextStore).toHaveProperty('data');
    });

    it('should have a test property in data', () => {
      expect(contextStore.data.test).toBe('1234');
    });
  });

  describe('methods', () => {
    it('should have a methods property', () => {
      expect(contextStore).toHaveProperty('methods');
    });

    it('should have a setTest function in methods', () => {
      expect(contextStore.methods.setTest).toBeInstanceOf(Function);
    });

    it('should call subject.next() when calling setText', () => {
      contextStore.methods.setTest('done');
      expect(contextStore.subject.next).toHaveBeenCalled();
    });
  });

  describe('subject', () => {
    it('should have a subject property', () => {
      expect(contextStore).toHaveProperty('subject');
    });

    it('should have a subject property to be instance of Subject', () => {
      expect(contextStore.subject).toBeInstanceOf(Subject);
    });
  });
});