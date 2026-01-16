"use strict";

module.exports = (email) => /^[-\w.]+@([A-z0-9][-A-z0-9]+\.)+[A-z]{2,4}$/i.test(email);