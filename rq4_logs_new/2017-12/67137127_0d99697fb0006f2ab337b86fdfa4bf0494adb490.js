var IdmCore = require('../index');
var clone = require('clone');
var assert = require('assert');
var deepdif = require('deep-diff');
var createError = require('http-errors');
var fs = require('fs');
var dbconnection = require('agile-idm-entity-storage').connectionPool;

var db;
var dbName = "./database";
var rmdir = require('rmdir');
var conf = require('./entity-policies-conf');
var dbName = conf.storage.dbName;

IdmCore.prototype.getPap = function () {
  return this.pap;
};

IdmCore.prototype.getStorage = function () {
  return this.storage;
}

var idmcore = new IdmCore(conf);

var group_name = "group";

var elisa = {
  "user_name": "elisa",
  "auth_type": "agile-local",
  "password": "secret",
  "role": "admin",
  "owner": "elisa!@!agile-local",
  "id": "elisa!@!agile-local",
  "type": "/user"
};
var elisa_auth = clone(elisa);
elisa_auth.id = "elisa!@!agile-local";
elisa_auth.type = "/user";

var bob = {
  "user_name": "bob",
  "auth_type": "agile-local",
  "password": "secret",
  "role": "admin",
  "owner": "bob!@!agile-local",
  "id": "bob!@!agile-local",
  "type": "/user"
};
var bob_auth = clone(bob);
bob_auth.id = "bob!@!agile-local";
bob_auth.type = "/user";

var admin = {
  "user_name": "admin",
  "auth_type": "agile-local",
  "password": "secret",
  "role": "admin",
  "owner": "admin!@!agile-local",
  "id": "admin!@!agile-local",
  "type": "/user"
};
var admin_auth = clone(admin);
admin_auth.id = "admin!@!agile-local";
admin_auth.type = "/user";

function cleanDb(c) {
  function disconnect(done) {
    dbconnection("disconnect").then(function () {
      rmdir(dbName + "_entities", function (err, dirs, files) {
        rmdir(dbName + "_groups", function (err, dirs, files) {
          rmdir(conf.upfront.pap.storage.dbName + "_policies", function (err, dirs, files) {
            done();
          });

        });
      });
    }, function () {
      throw Error("not able to close database");
    });
  }

  disconnect(c);
}

function buildUsers(done) {
  // DEBUG
  // console.log("#### DEBUG INFORMATION FROM buildUsers()");
  var arr = [idmcore.getPap().setDefaultEntityPolicies(admin_auth.id, admin_auth.type),
    idmcore.getStorage().createEntity(admin_auth.id, admin_auth.type, admin_auth.id, admin)
  ];

  // Sets default entity policies and creates user 'admin'
  Promise.all(arr)
    .then(function (read) {
      // DEBUG
      // console.log("User '" + read[1].id + "' was successfully created!");
      // Creates user 'bob'
      return idmcore.createEntityAndSetOwner(admin_auth, bob_auth.id, bob_auth.type, bob, bob_auth.id);
    }).then(function (read) {
      // DEBUG
      // console.log("User '" + read.id + "' was successfully created!");
      // Creates user 'elsia'
      return idmcore.createEntityAndSetOwner(admin_auth, elisa_auth.id, elisa_auth.type, elisa, elisa_auth.id);
    }).then(function (read) {
      // DEBUG
      // console.log("User '" + read.id + "' was successfully created!");
      // Creates group 'group'
      return idmcore.createGroup(admin_auth, group_name);
    })
    .then(function (read) {
      // DEBUG
      // console.log("Group '" + read.group_name + "' was successfully created and is owned by '" + read.owner + "'!");
      // Adds user 'admin' to group 'group'
      return idmcore.addEntityToGroup(admin_auth, group_name, admin_auth.id, bob_auth.id, bob_auth.type);
    })
    .then(function (read) {
      // DEBUG
      // console.log("User '" + read.id + "' was successfully added to group '" + read.groups[0].group_name + "'!");
      // Fulfills promise
      done();
    }, function (err) {
      throw err;
    });
}

describe('GROUP ACTIONS', function () {

    beforeEach(function (done) {
      buildUsers(done);
    });

    afterEach(function (done) {
      cleanDb(done);
    });

    /*
     * elisa is the owner of its own entity
     * she should be able to read the password
     */
    it('should return user entity "elisa" with password attribute for owner elisa', function (done) {
      // policies for user attribute password
      var policies = [
        {
          "op": "read",
          "locks":
          [
            {
              "lock": "isOwner"
            }
          ]
        }, {
          "op": "read",
          "locks":
          [
            {
              "lock": "isGroupMember",
              "args": ["group"]
            }
          ]
        }, {
          "op": "write"
        } ];
      // DEBUG
      // console.log("#### DEBUG INFORMATION FROM 'test'");
      idmcore.setMocks(null, null, null, dbconnection);
      // Sets password policy for elisa. Password can be read by the owner and a member of the group 'group'.
      idmcore.setEntityPolicy(elisa_auth, elisa_auth.id, elisa_auth.type, "password", policies)
        .then(function (res) {
          // DEBUG
          // if (res != null) {
          //   console.log("Policies were successfully applied!");
          // }
          // Retrieves user entity elisa
          // console.log(elisa_auth);
          return idmcore.readEntity(elisa_auth, elisa_auth.id, elisa_auth.type);
        }).then(function (res) {
          // DEBUG
          // console.log(JSON.stringify(res));
          // Fulfills promise
          if (res.hasOwnProperty("password")) {
            done();
          }
        }).catch(function (err) {
          throw err;
        });
    });

    /*
     * admin is another user. he is not the owner of the user entity 'elisa' and not a member of group 'group'
     * he should not be able to read the password
     */
    it('should return user entity "elisa" without password attribute for user admin', function (done) {
      // policies for user attribute password
      var policies = [
        {
          "op": "read",
          "locks":
          [
            {
              "lock": "isOwner"
            }
          ]
        }, {
          "op": "read",
          "locks":
          [
            {
              "lock": "isGroupMember",
              "args": ["group"]
            }
          ]
        }, {
          "op": "write"
        } ];
      // DEBUG
      // console.log("#### DEBUG INFORMATION FROM 'test'");
      idmcore.setMocks(null, null, null, dbconnection);
      // Sets password policy for elisa. Password can be read by the owner and a member of the group 'group'.
      idmcore.setEntityPolicy(elisa_auth, elisa_auth.id, elisa_auth.type, "password", policies)
        .then(function (res) {
          // DEBUG
          // if (res != null) {
          //   console.log("Policies were successfully applied!");
          // }
          // Retrieves user entity elisa
          // console.log(elisa_auth);
          return idmcore.readEntity(admin_auth, elisa_auth.id, elisa_auth.type);
        }).then(function (res) {
          // DEBUG
          // console.log(JSON.stringify(res));
          // Fulfills promise
          if (!res.hasOwnProperty("password")) {
            done();
          }
        }).catch(function (err) {
          throw err;
        });
    });

    /*
     * bob is member of group 'group'.
     * he should be able to read the password, while he is not the owner of the user entity, but member of the group 'group'.
     */
    it('should return user entity "elisa" with password attribute for group member bob', function (done) {
      // policies for user attribute password
      var policies = [
        {
          "op": "read",
          "locks":
          [
            {
              "lock": "isOwner"
            }
          ]
        }, {
          "op": "read",
          "locks":
          [
            {
              "lock": "isGroupMember",
              "args": ["group"]
            }
          ]
        }, {
          "op": "write"
        } ];

      idmcore.setMocks(null, null, null, dbconnection);
      // Sets password policy for elisa. Password can be read by the owner and a member of the group 'group'.
      idmcore.setEntityPolicy(bob_auth, elisa_auth.id, elisa_auth.type, "password", policies)
        .then(function (res) {
          // DEBUG
          // if (res != null) {
          //   console.log("Policies were successfully applied!");
          // }
          // Retrieves user entity bob from database, otherwise the group relation would be missing
          var bob_saved = idmcore.readEntity(bob_auth, bob_auth.id, bob_auth.type);
          return bob_saved;
        }).then(function (bob_saved) {
          // DEBUG
          // console.log("User '" + bob_saved.id + "' was successfully retrieved from the database!");
          // Read user entity elisa
          return idmcore.readEntity(bob_saved, elisa_auth.id, elisa_auth.type);
        }).then(function (res) {
          // DEBUG
          // console.log(JSON.stringify(res));
          // Fulfills promise
          if (res.hasOwnProperty("password")) {
            done();
          }
        }).catch(function (err) {
          throw err;
        });
    });

});