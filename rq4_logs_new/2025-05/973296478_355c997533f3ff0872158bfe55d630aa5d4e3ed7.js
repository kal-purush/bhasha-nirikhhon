const db = require("../db/mysql");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");

exports.getUserByEmail = async (email) => {
    const [existingUsers] = await db.query(
        "SELECT * FROM users WHERE email = ?",
        [email]
    );

    return existingUsers[0];
};

exports.getUserById = async (id) => {
    const [existingUsers] = await db.query(
        "SELECT * FROM users WHERE id = ?",
        [id]
    );

    return existingUsers[0];
};

exports.createUser = async (user) => {
    const hashedPassword = await bcrypt.hash(user.password, 10);

    const [addressResult] = await db.query(
        "INSERT INTO address (city, postcode, street) VALUES (?, ?, ?)",
        [user.city, user.postcode, user.street]
    );
    const addressId = addressResult.insertId;

    const [userResult] = await db.query(
        "INSERT INTO users (name, surname, email, password, address_id) VALUES (?, ?, ?, ?, ?)",
        [user.name, user.surname, user.email, hashedPassword, addressId]
    );

    return userResult.insertId;
};

exports.generateUserJWT = async (user) => {
    const token = jwt.sign(
      {
        id: user.id,
        email: user.email,
        name: user.name,
        surname: user.surname,
      },
      process.env.JWT_SECRET,
      { expiresIn: "1h" }
    );

    return token;
}

exports.deleteUser = async (id) => {
    const [result] = await db.query(
        "DELETE FROM users WHERE id = ?",
        [id]
    );
    return result.affectedRows > 0;
};

exports.updateUser = async (id, { name, surname, email, city, postcode, street }) => {
    const [users] = await db.query("SELECT address_id FROM users WHERE id = ?", [id]);
    
    if (users.length === 0) {
        throw new Error("User not found");
    }

    const addressId = users[0].address_id;

    await db.query(
        "UPDATE users SET name = ?, surname = ?, email = ? WHERE id = ?",
        [name, surname, email, id]
    );

    await db.query(
        "UPDATE address SET city = ?, postcode = ?, street = ? WHERE id = ?",
        [city, postcode, street, addressId]
    );

    return true;
};

exports.updateUserPassword = async (id, hashedPassword) => {
    await db.query(
        "UPDATE users SET password = ? WHERE id = ?",
        [hashedPassword, id]
    );
    return true;
};