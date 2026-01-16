import * as mocha from "mocha";
import chai, { expect } from "chai";
import chaiHTTP from "chai-http";
import server from "src/server";

chai.use(chaiHTTP);

mocha.describe("Server Setup", function MochaTest() {
  this.timeout(5000);
  mocha.it("Test if server is running", (done) => {
    chai
      .request(server)
      .get("/")
      .then((r) => {
        expect(r.status).to.be.equal(200);
        done();
      });
  });
});