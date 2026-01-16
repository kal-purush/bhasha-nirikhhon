const db = require("../data/dbConfig.js");

module.exports = {
    addNewPhoto,
    getAllPhotos,
    findBy,
    getPhotoById,
    update,
    remove
};

function getAllPhotos() {
    return db("photos");
}

function getPhotoById(id) {
    return db("photos")
        .where({ id })
        .first();
}

function findBy(filter) {
    return db("photos").where(filter);
}

async function addNewPhoto(photo) {
    const [id] = await db("photos").insert(photo, "id");

    return getPhotoById(id);
}

function update(id, changes) {
    return db("photos")
        .where({ id })
        .update(changes);
}

function remove(id) {
    return db("photos")
        .where({ id })
        .del();
}