
app.factory('UserService', ['$resource', ($resource) => {
  return $resource('/api/users/:user_id', {'user_id': '@user_id'})
}])