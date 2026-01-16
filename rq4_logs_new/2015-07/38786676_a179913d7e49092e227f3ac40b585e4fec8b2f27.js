var request = require('request')
  , expect = require('chai').expect
  , cheerio = require('cheerio')

var users = require("../app.js")  //linking the pages together

// DESCRIBE WHAT WE ARE TESTING
  // SAY WHAT BEHAVIOR 'IT' AUGHT TO HAVE
    // SEND THE REQUEST
      // USE CHAI-EXPECT TO EXPECT THE STATUS RESULT
      // CHECK FALSE VALUE TO SEE IF WE CAN MAKE TEST FAIL
      // CALL DONE();
// GOOGLE - PROVIDED STARTER
describe('Google.com', function() {
  it('should have a HTTP of 200 - success', function(done) {
    request('https://google.com/', function(err, res, body) {
      expect(res.statusCode).to.equal(200)
      // expect(res.statusCode).to.equal(300)
      console.log("err " + err + "res " + res + "body " + body);
   done();
    });
  });
});
// AMAZON 
// describe('Amazon.com', function() {
//   it('should have a HTTP of 200 - success', function(done) {
//     request('https://www.amazon.com/', function(err, res, body) {
//       expect(res.statusCode).to.equal(200)
//       // expect(res.statusCode).to.equal(300)
//       console.log("err " + err + "res " + res + "body " + body);

//       done();
//     });
//   });
// });

// STARTING THE LOCALHOST
describe('localhost API TESTING', function() {
  it('should have a HTTP of 200 - success', function(done) {
    request('http://localhost:3000/users', function(err, res, body) {
      expect(res.statusCode).to.equal(200);
      // expect(res.statusCode).to.equal(300)
      // console.log("err " + err + "res " + res + "body " + body);

      done();
    });
  });
});

// POSTING THE USER
var user = {}
describe('localhost API TESTING', function() {
  it('should have a HTTP of 200 - success', function(done) {form:{
    user = {id: '7',
  username: "hi",
  firstname: "bob",
  lastname: "dylan",
  age: 20
  }};
  // you have to use this bc it's a diff kind of post
    request.post('http://localhost:3000/users/', {form: user}, function(err, res, body) {
      expect(res.statusCode).to.equal(200);
      // expect(res.statusCode).to.equal(300)
      console.log("NEW USER" + user);
      done();
    });
  });
});

// PUT THE USER 
describe('Putting the user', function() {
  it('should have a HTTP of 200 - success', function(done) {
    //user.username = "hello"

  request.put('http://localhost:3000/users/7', {form: user}, function(err, res, body) {

    expect(res.statusCode).to.equal(200);
    console.log("PUT THE USER", user);
    done();
    });
  });
});


// DELETING THE USER
describe('Deleting the user', function() {
  it('should have a HTTP of 200 - success', function(done) {
    // var deleted;
  request.del('http://localhost:3000/users/1', {form: {id: 1}}, function(err, res, body) {
      deleted = body;
    expect(res.statusCode).to.equal(200);
    console.log("USER DELETED", user)
    done();
    });
  });
});























