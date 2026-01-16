"use strict";

const { CODES, MESSAGES } = require("./statusCodes");

const {
  getAllFromStorage,
  getFromStorage,
  addToStorage,
  updateStorage,
  removeFromStorage,
} = require("./storageLayer");

//Datastorage class

module.exports = class Datastorage {
  get CODES() {
    return CODES;
  }

  getAll() {
    return getAllFromStorage();
  } //end getAll

  getOne(name) {
    return new Promise(async (resolve, reject) => {
      if (!name) {
        reject(MESSAGES.NOT_FOUND("---empty---"));
      } else {
        const result = await getFromStorage(name);
        if (result) {
          resolve(result);
        } else {
          reject(MESSAGES.NOT_FOUND(name));
        }
      }
    });
  } //end of getOne

  insert(word) {
    return new Promise(async (resolve, reject) => {
      if (word) {
        if (!word.name) {
          reject(MESSAGES.NOT_INSERTED());
        } else if (await getFromStorage(word.name)) {
          reject(MESSAGES.ALREADY_IN_USE(word.name));
        } else if (await addToStorage(word)) {
          resolve(MESSAGES.INSERT_OK(word.name));
        } else {
          reject(MESSAGES.NOT_INSERTED());
        }
      } else {
        reject(MESSAGES.NOT_INSERTED());
      }
    });
  } //end of insert

  update(word) {
    return new Promise(async (resolve, reject) => {
      if (word) {
        if (await updateStorage(word)) {
          resolve(MESSAGES.UPDATE_OK(word.name));
        } else {
          reject(MESSAGES.NOT_UPDATED());
        }
      } else {
        reject(MESSAGES.NOT_UPDATED());
      }
    });
  } //end update

  remove(name) {
    return new Promise(async (resolve, reject) => {
      if (!name) {
        reject(MESSAGES.NOT_FOUND("---empty---"));
      } else if (await removeFromStorage(name)) {
        resolve(MESSAGES.REMOVE_OK(name));
      } else {
        reject(MESSAGES.NOT_REMOVED(name));
      }
    });
  } //end of remove
};