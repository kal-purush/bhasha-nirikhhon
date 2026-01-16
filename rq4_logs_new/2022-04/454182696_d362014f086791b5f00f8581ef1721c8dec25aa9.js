const { User } = require('../../models');

module.exports = async function userAuthentication(email, password) {
  const user = await User.findOne({ where: { email, password }});

  return user;
}