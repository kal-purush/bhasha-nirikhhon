import * as Firebase from 'firebase'

import { HasChildrenPipe } from '../../src/has_children_pipe'

export function main(FIREBASE_URL: string) {
  describe('HasChildrenPipe', () => {
    var firebase: Firebase;
    var pipe: HasChildrenPipe;

    beforeEach(() => {
      firebase = new Firebase(FIREBASE_URL);
      pipe = new HasChildrenPipe();
    });

    it("should return false when given null", () => {
      expect(pipe.transform(null)).toBe(false);
    });

    it("should return false when snapshot hasn't any children", done => {
      firebase.child('dinosaurs/tyrannosaurus').once('value', snapshot => {
        expect(pipe.transform(snapshot)).toBe(false);
        done();
      });
    });

    it("should return true when snapshot has any children", done => {
      firebase.child('scores').once('value', snapshot => {
        expect(pipe.transform(snapshot)).toBe(true);
        done();
      });
    });
  });
}