import * as Firebase from 'firebase'

import { toFirebase } from '../../src/utils/to_firebase'

export function main() {
  let firebaseUrl = 'ws://test.firebaseio.com:5000';

  describe('toFirebase', () => {
    it ('should return a new Firebase reference', () => {
      let firebaseRef = toFirebase(firebaseUrl);

      expect(firebaseRef).toEqual(jasmine.any(Firebase));
      expect(firebaseRef.toString()).toBe(firebaseUrl);
    });

    it ('should create a new Firebase reference', () => {
      let anotherFirebaseRef = new Firebase(firebaseUrl);
      let firebaseRef = toFirebase(anotherFirebaseRef);

      expect(firebaseRef).toEqual(jasmine.any(Firebase));
      expect(firebaseRef).not.toBe(anotherFirebaseRef);
      expect(firebaseRef.toString()).toBe(firebaseUrl);
    });
  });
}