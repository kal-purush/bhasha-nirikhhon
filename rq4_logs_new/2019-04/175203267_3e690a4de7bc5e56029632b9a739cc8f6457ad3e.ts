import { Server } from './../server';
import { AddCardRoute } from './../routes/add-card';
import { RouteTestSuite } from "./route-test-suite";

import { expect } from "chai";
import { exampleValidClientId, exampleValidCard, exampleValidPin } from './validators';
import { VerifyPinByCardNumberRoute } from '../routes/verify-pin-card-number';
import { Log } from './log';

import * as fs from "fs";
import "mocha";

const removeLog = () => {
  try {
    fs.unlinkSync("log.json");
  } catch (e) {}
}

let server: Server;

describe("Log tests", () => {

  before(done => {
    server = new Server();
    server.start().then(done);
  });

  after(done => {
    server.stop().then(done);
  });

  it("Should only ever have one instance", () => {
    expect(Log.getInstance()).to.equal(Log.getInstance());
  });

  it("Should automatically recreate logfile", () => {
    removeLog();
    Log.getInstance().addLogItem([{
      timestamp: Date.now()
    }]);

    expect(fs.existsSync("log.json")).to.equal(true);
  });

  it("Should add logs to logfile", () => {
    removeLog();

    for (let i = 0; i < 10; i++) {
      Log.getInstance().addLogItem({
        timestamp: Date.now()
      });
    }

    expect(Log.getInstance().getLogLength()).to.equal(10);
  });

  it("Should log requests", (done) => {
    (async () => {
      removeLog();

      await RouteTestSuite.request(new AddCardRoute(), {
        clientId: exampleValidClientId
      });

      expect(Log.getInstance().getLogLength()).to.equal(1);
      
      done();
    })();
  });

  it("Should redact pins", done => {
    (async () => {
      removeLog();

      await RouteTestSuite.request(new VerifyPinByCardNumberRoute(), {
        cardNumber: exampleValidCard,
        pin: exampleValidPin
      })

      const log = Log.getInstance().getLog();
      console.log(log);
      expect(log[0].url).to.not.contain(`pin=${exampleValidPin}`);
      expect(log[0].query.pin).to.equal(undefined);
      done();
    })();
  });

  // it("Should send requests to server when over 100", done => {
  //   (async () => {
  //     await RouteTestSuite.request(new AddCardRoute(), {
  //       clientId: exampleValidClientId
  //     });

  //     expect(Log.getInstance().getLogLength()).to.equal(0);

  //     done();
  //   })();
  // });

});