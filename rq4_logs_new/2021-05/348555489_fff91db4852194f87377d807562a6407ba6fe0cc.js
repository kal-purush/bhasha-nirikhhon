const guests = require('../repositories/guests')
module.exports = {
  find() {
    let guests = await guests.findAll();
    return guests;
  },

  find(id) {
    let guests = await guests.find(id);
    return guests;
  },

  create(id) {
    let guest = await guests.create(id);
    return guests;
  },

  update(body) {
    let guest = await guests.update(body.token, body.id)
    return guest;
  }
}