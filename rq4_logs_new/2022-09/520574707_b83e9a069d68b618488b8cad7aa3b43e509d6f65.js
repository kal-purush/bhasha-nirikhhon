'use strict';
const { getConnection } = require('../db/db');

const likePhoto = async (req, res, next) => {
  let connection;

  try {
    connection = await getConnection();

    // Recojo los parámetros
    const { id } = req.params;
    const { vote } = req.body;

    // Compruebo el usuario no sea el creador de la entrada
    const [current] = await connection.query(
      `
      SELECT user_id 
      FROM photo
      WHERE id=?
    `,
      [id]
    );

    if (current[0].user_id === req.userAuth.id) {
      const error = new Error('No puedes votar tu propio post');
      error.httpStatus = 403;
      throw error;
    }

    // Compruebo que el usuario no votara anteriormente la entrada
    const [existingVote] = await connection.query(
      `
      SELECT id
      FROM photo_votes
      WHERE user_id=? AND photo_id=?
    `,
      [req.userAuth.id, id]
    );

    if (existingVote.length > 0) {
      `
      DELETE INTO likes(date, vote, photo_id, user_id)
      VALUES(?,?,?,?)
    `;
    }

    // Añado el voto a la tabla
    await connection.query(
      `
      INSERT INTO likes(date, vote, photo_id, user_id)
      VALUES(?,?,?,?)
    `,
      [vote, id, req.userAuth.id]
    );

    // Saco la nueva media de votos (por implementar)

    //   res.send({
    //     status: 'ok',
    //     data: {
    //       likes:
    //     },
    //   });
  } catch (error) {
    next(error);
  } finally {
    if (connection) connection.release();
  }
};

module.exports = likePhoto;