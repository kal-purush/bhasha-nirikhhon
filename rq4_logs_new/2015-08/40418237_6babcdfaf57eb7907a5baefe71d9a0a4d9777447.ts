import {expect} from 'chai';
import {Generator, Transform} from '../../src/core';
import {FilterSubject, Observable} from '../../src/async/observables';

describe('Filter Subject', () => {

  it('should only emit objects that compose to true', done => {
    let sequence: number[] = [1, 2, 3, 4];
    let composition: number[] = [2, 4];
    let cb: Generator<number> = observer => {
      sequence.forEach(object => {
        observer.next(object);
      });
    };
    let transform: Transform<number, boolean> = object => {
      return object % 2 === 0;
    };
    let observable: Observable<number> = Observable.create<number>(cb);
    let subject: FilterSubject<number>
        = new FilterSubject<number>(observable, transform);
    subject.subscribeOnNext(object => {
      expect(object % 2 === 0).to.be.true;
      expect(composition.indexOf(object)).to.be.above(-1);
      composition.splice(composition.indexOf(object), 1);
      if (composition.length === 0) {
        done();
      }
    });
  });

  it('should be disposed after its subscription completes', done => {
    let cb: Generator<void> = observer => {
      observer.complete();
    };
    let transform: Transform<void, boolean> = () => {
      return true;
    };
    let observable: Observable<void> = Observable.create<void>(cb);
    let subject: FilterSubject<void>
        = new FilterSubject<void>(observable, transform);
    subject.subscribeOnComplete(() => {
      expect(subject.isDisposed).to.be.true;
      subject._scheduler.schedule<void>([null], () => {
        expect(subject._subscribers).to.be.empty;
        done();
      });
    });
  });

  it('should throw an error if it calls next after being disposed', done => {
    let observable: Observable<void> = Observable.create<void>(null);
    let subject: FilterSubject<void>
        = new FilterSubject<void>(observable, null);
    subject.complete(observable);
    try {
      subject.next();
    } catch (err) {
      done();
    }
  });

  it('should catch any error throw inside of its transform', done => {
    let zero: number = 0;
    let cb: Generator<number> = observer => {
      observer.next(zero);
    };
    let transform: Transform<number, boolean> = (object): boolean => {
      throw new Error('filter subject transform catch test');
    };
    let observable: Observable<number> = Observable.create<number>(cb);
    let subject: FilterSubject<number>
        = new FilterSubject<number>(observable, transform);
    subject.subscribeOnError(err => done());
  });

});