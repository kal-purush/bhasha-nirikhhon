require("../server/db");
const { Member } = require("../server/db/models");
const userSeed = require("../src/seedData/userSeedData");

userSeed.forEach((e) => {
  Member.create(e, function (err, member) {
    if (err) {
      throw err;
    } else {
      console.log('member create:', member.mundaneFirstName)
    }
  });
});