import CallbackManager from '../../app/launcher/CallbackManager';
import { assert } from 'chai';

/* eslint-env mocha */

describe('CallbackManagerTest', () => {
  it('simple post', (done) => {
    let myCallback: (data: any) => void;
    const cm = new CallbackManager((data) => {
      setTimeout(() => {
        myCallback(data);
      }, data.delay || 0);
    })
    myCallback = cm.getRecieveCallback();

    cm.post({ msg: 100, delay: 0 }, (data) => {
      assert.equal(data.msg, 100, "data lost");
      done();
    });
  });
  it('twice post', (done) => {
    let myCallback: (data: any) => void;
    const cm = new CallbackManager((data) => {
      setTimeout(() => {
        myCallback(data);
      }, data.delay || 0);
    })
    myCallback = cm.getRecieveCallback();

    let counter = 0;
    cm.post({ msg: 100, delay: 10 }, (data) => {
      assert.equal(data.msg, 100, "data lost");
      if (++counter == 2) done();
    });
    cm.post({ msg: 200, delay: 20 }, (data) => {
      assert.equal(data.msg, 200, "data lost");
      if (++counter == 2) done();
    });
  });
  it('twice post cross', (done) => {
    let myCallback: (data: any) => void;
    const cm = new CallbackManager((data) => {
      setTimeout(() => {
        myCallback(data);
      }, data.delay || 0);
    })
    myCallback = cm.getRecieveCallback();

    let counter = 0;
    cm.post({ msg: 100, delay: 20 }, (data) => {
      assert.equal(data.msg, 100, "data lost");
      if (++counter == 2) done();
    });
    cm.post({ msg: 200, delay: 10 }, (data) => {
      assert.equal(data.msg, 200, "data lost");
      if (++counter == 2) done();
    });
  });
});