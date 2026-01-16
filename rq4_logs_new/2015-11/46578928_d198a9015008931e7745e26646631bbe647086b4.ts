import * as Firebase from 'firebase'

import { NumChildrenPipe } from '../../src/num_children_pipe'

export function main(FIREBASE_URL: string) {
  describe('NumChildrenPipe', () => {
    var firebase: Firebase;
    var pipe: NumChildrenPipe;

    beforeEach(() => {
      firebase = new Firebase(FIREBASE_URL);
      pipe = new NumChildrenPipe();
    });

    it("should return 0 when given null", () => {
      expect(pipe.transform(null)).toBe(0);
    });

    it("should return 0 when snapshot hasn't any children", done => {
      firebase.child('dinosaurs/tyrannosauruss').once('value', snapshot => {
        expect(pipe.transform(snapshot)).toBe(0);
        done();
      });
    });

    it("should return 0 when snapshot valie is a primitive", done => {
      firebase.child('dinosaurs/lambeosaurus/weight').once('value', snapshot => {
        expect(pipe.transform(snapshot)).toBe(0);
        done();
      });
    });

    it("should return the number of children", done => {
      firebase.child('scores').once('value', snapshot => {
        expect(pipe.transform(snapshot)).toBe(6);
        done();
      });
    });
  });
}