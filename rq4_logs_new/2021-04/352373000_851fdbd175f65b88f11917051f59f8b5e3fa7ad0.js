import chai from "chai";
import request from "supertest";
import dotenv from 'dotenv';

let api ;

dotenv.config();

const expect = chai.expect;

describe("products endpoint", function () {
  this.timeout(5000);
  beforeEach(async() => {
    try {
      api = require("../../../../index");  

    } catch (err) {
      console.error(`failed to Load index: ${err}`);
    }
  });

  afterEach(() => {
    delete require.cache[require.resolve("../../../../index")];
  });

  describe("GET /products ", () => {
    it("should response shoes ", (done) => {
      request(api)
        .get("/api/products/shoes")
        .end((err, res) => {
            // console.log(res.body);
          if (err) return done(err);
          expect(parseInt(res.body.storage)).to.be.at.least(1);
          expect(res.body.item_name).equals("Nike concept sneaker");
          done();
        });
    });

    it("should response scale ", (done) => {
        request(api)
          .get("/api/products/scale")
          .end((err, res) => {
              // console.log(res.body);
            if (err) return done(err);
            expect(parseInt(res.body.storage)).to.be.at.least(1);
            expect(res.body.item_name).equals("Xiaomi Mi Smart Scales");
            done();
          });
      });

      it("should response watch ", (done) => {
        request(api)
          .get("/api/products/watch")
          .end((err, res) => {
              // console.log(res.body);
            if (err) return done(err);
            expect(parseInt(res.body.storage)).to.be.at.least(1);
            expect(res.body.item_name).equals("Huawei GT2 Sport Watch");
            done();
          });
      });
  });


 



});