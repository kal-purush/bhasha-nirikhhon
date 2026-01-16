const db = require('../../data/db-config');

const getItems = async (userId) => {
  return await db('trip_items').where({ user_id: userId });
};

const getTripItems = async (userId, tripId) => {
  return await db('trip_items').where({ user_id: userId, trip_id: tripId });
};

const addItem = (item) => {
  return db('trip_items').insert(item).returning('*');
};

const removeItem = async (item_id) => {
  return db('trip_items').where({ id: item_id }).delete();
};

const updateItem = async (item_id, changes) => {
  return db('trip_items').where({ id: item_id }).update(changes).returning('*');
};
module.exports = {
  getItems,
  getTripItems,
  addItem,
  removeItem,
  updateItem,
};