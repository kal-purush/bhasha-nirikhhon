function (user, context, callback) {
  // The currently requested scopes can be accessed as follows:
  // context.request.query.scope.match(/\S+/g)
  var scopeMapping = {
    contactInfo: ["givenName", "familyName", "email", "nickname", "picture"],
    phones: ["user_metadata.phone1", "user_metadata.phone2"]
  };
  context.jwtConfiguration.scopes = scopeMapping;
  callback(null, user, context);
}