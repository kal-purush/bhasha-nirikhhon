Meteor.startup(function() {  

  if (Meteor.isClient || !process.env.IS_MIRROR) {
    return;
  }

  Meteor.users.remove({});

	/*
  Accounts.createUser({
    username: "admin",
    email: "admin@satellid.com",
    password: "isenseastrongpassword"
  });
	*/

});
