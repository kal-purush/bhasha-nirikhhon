var expect = require('chai').expect;
var news = require("./news.js");

describe('top-news', function() {
    describe('allNews', function() {
      it('should be a non-empty array', function() {
        expect(news.allNews).not.to.have.length(0);
      });
    });
  
  });