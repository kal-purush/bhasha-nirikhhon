import * as Firebase from 'firebase'

import { toFirebase } from '../src/utils/to_firebase'

export function main() {
  let firebaseUrl = "ws://test.firebaseio.com:5000";

  describe('toFirebase', () => {
    it ('should return a new Firebase reference', () => {
      let firebaseRef = toFirebase(firebaseUrl);

      expect(firebaseRef).toEqual(jasmine.any(Firebase));
    });

    it ('should not create a new Firebase reference', () => {
      let firebaseRef = new Firebase(firebaseUrl);

      expect(toFirebase(firebaseRef)).toEqual(firebaseRef);
    });
  });
}