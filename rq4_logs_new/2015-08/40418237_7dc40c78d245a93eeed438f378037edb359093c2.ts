import { expect } from 'chai';
import { Generator, Observer } from '../../src/core';
import { ColdObservable } from '../../src/async/observable';

describe('Cold Observable', () => {

  it('should call its generator for each new subscription', done => {
    let zero: number = 0;
    let cb: Generator<number> = observer => observer.next(zero);
    let observable: ColdObservable<number> = new ColdObservable<number>(cb);
    observable.subscribeOnNext(firstItem => {
      expect(firstItem).to.equal(zero);
      observable.subscribeOnNext(secondItem => {
        expect(secondItem).to.equal(zero);
        done();
      });
    });
  });

  it('should emit all notifications to its subscription', done => {
    let sequence: number[] = [ 1, 2, 3 ];
    new ColdObservable<number>(observer => sequence.forEach(object => observer.next(object)))
      .subscribeOnNext(object => {
        expect(sequence.indexOf(object)).to.be.above(-1);
        sequence.splice(sequence.indexOf(object), 1);
        if (sequence.length === 0) done();
      });
  });

  it('should emit all errors to its subscription', done => {
    let sequence: number[] = [ 1, 2, 3 ];
    new ColdObservable<number>(observer => sequence.forEach(object => observer.error(object)))
      .subscribeOnError(object => {
        expect(sequence.indexOf(object)).to.be.above(-1);
        sequence.splice(sequence.indexOf(object), 1);
        if (sequence.length === 0) done();
      });
  });

  it('should complete its subscription upon completion', done => {
    new ColdObservable<void>(observer => observer.complete())
      .subscribeOnComplete(() => done());
  });

  it('should pass a reference on complete', done => {
    let observable: ColdObservable<void> = new ColdObservable<void>(observer => observer.complete());
    observable.subscribeOnComplete(subscription => {
      expect(subscription).to.equal(observable);
      done();
    });
  });

  it('should fire notifications asynchronously', done => {
    let flag: boolean = false;
    let completeFlag: boolean = false;
    let errorFlag: boolean = false;
    let nextFlag: boolean = false;
    new ColdObservable<void>(observer => observer.complete())
      .subscribeOnComplete(() => {
        expect(flag).to.be.true;
        completeFlag = true;
        if (completeFlag && errorFlag && nextFlag) done();
      });
    new ColdObservable<void>(observer => observer.error(null))
      .subscribeOnError(() => {
        expect(flag).to.be.true;
        errorFlag = true;
        if (completeFlag && errorFlag && nextFlag) done();
      });
    new ColdObservable<void>(observer => observer.next())
      .subscribeOnNext(() => {
        expect(flag).to.be.true;
        nextFlag = true;
        if (completeFlag && errorFlag && nextFlag) done();
      });
    flag = true;
  });

  it('should throw an error if a completed observable is subscribed to', done => {
    let cb: Generator<void> = observer => observer.complete();
    let observable: ColdObservable<void> = new ColdObservable<void>(cb);
    let subscriber: Observer<void> = {
      complete: () => observable.subscribe(null),
      error: err => done(),
      next: () => {}
    };
    observable.subscribe(subscriber);
  });

  it('should throw an error if a completed observer calls complete', done => {
    let cb: Generator<void> = observer => {
      observer.complete();
      observer.complete();
    };
    new ColdObservable<void>(cb).subscribeOnError(err => done());
  });

  it('should throw an error if a completed observer calls error', done => {
    let cb: Generator<void> = observer => {
      observer.complete();
      observer.error(null);
    };
    new ColdObservable<void>(cb).subscribeOnError(err => done());
  });

  it('should throw an error if a completed observer calls next', done => {
    let cb: Generator<void> = observer => {
      observer.complete();
      observer.next();
    };
    new ColdObservable<void>(cb).subscribeOnError(err => done());
  });

  it('should catch any error thrown inside of a generator', done => {
    new ColdObservable<void>(observer => {
        throw new Error('cold generator catch test');
      })
      .subscribeOnError(err => done());
  });

  it('should catch any error thrown inside of a complete call', done => {
    let subscriber: Observer<void> = {
      complete: () => { throw new Error('cold complete catch test'); },
      error: err => done(),
      next: () => {}
    };
    new ColdObservable<void>(observer => observer.complete())
      .subscribe(subscriber);
  });

  it('should catch any error thrown inside of a next call', done => {
    let subscriber: Observer<void> = {
      complete: () => {},
      error: err => done(),
      next: () => { throw new Error('cold next catch test'); }
    };
    new ColdObservable<void>(observer => observer.next())
      .subscribe(subscriber);
  });

});