import * as Firebase from 'firebase'

import { toFirebase } from '../../../src/utils/to_firebase'

export function main(FIREBASE_URL: string) {
  describe('toFirebase', () => {
    describe('when ref', () => {
      it ('should create a Firebase instance', () => {
        expect(toFirebase(FIREBASE_URL)).toEqual(jasmine.any(Firebase));
      });

      it ('should be mounted to the correct url', () => {
        expect(toFirebase(FIREBASE_URL).toString()).toEqual(FIREBASE_URL);
      });
    });

    describe('when url', () => {
      var firebase: Firebase;

      beforeEach(() => {
        firebase = new Firebase(FIREBASE_URL);
      });

      it ('should create a new Firebase instance', () => {
        let cloned = toFirebase(firebase);

        expect(cloned).toEqual(jasmine.any(Firebase));
        expect(cloned).not.toBe(firebase);
      });

      it ('should be mounted to the correct url', () => {
        expect(toFirebase(firebase).toString()).toEqual(FIREBASE_URL);
      });
    });
  });
}