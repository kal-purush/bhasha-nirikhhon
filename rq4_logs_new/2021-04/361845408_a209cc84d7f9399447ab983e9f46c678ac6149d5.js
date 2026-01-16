const db = require('../data/db-config')

//get all users with their id and username
function find() {
    return db("rentals")
    .select(
      "rental_id",
      "rental_period",
      "renter_id",
      "owner_id",
      "tech_item_id"
      ).orderBy("rental_id");
  }

//get rental by filter
function findBy(filter){
    return db("rentals").where(filter).orderBy("rental_id")
}

//get rental by id
function findById(rental_id) {
    return db("rentals").where(rental_id, "rental_id").first();
  }

//add rental to db, return the rental by id
async function add(rental) {
    const [rental_id] = await db("rentals").insert(rental, ["rental_id"]);
    return findById(rental_id);
}

module.exports = {
    find,
    findBy,
    findById,
    add
}