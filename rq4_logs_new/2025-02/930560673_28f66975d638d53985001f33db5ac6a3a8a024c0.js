import { db } from "../db.js";

export const getUsers = (_, res) => {
  const q = "SELECT * FROM evento";

  db.query(q, (err, data) => {
    if (err) return res.json(err);

    return res.status(200).json(data);
  });
};

export const addUser = (req, res) => {
  const q =
    "INSERT INTO evento(`Primeiro_Evento`, `Primeiro_Duracao`, `Segundo_Evento`, `Segundo_Duracao`,`Terceiro_Evento`,`Terceiro_Duracao`,`Quarto_Evento`,`Quarto_Duracao`) VALUES(?)";

  const values = [
    req.body.Primeiro_Evento,
    req.body.Primeiro_Duracao,
    req.body.Segundo_Evento,
    req.body.Segundo_Duracao,
    req.body.Terceiro_Evento,
    req.body.Terceiro_Duracao,
    req.body.Quarto_Evento,
    req.body.Quarto_Duracao,
  ];

  db.query(q, [values], (err) => {
    if (err) return res.json(err);

    return res.status(200).json("Evento criado com sucesso.");
  });
};

export const deleteUser = (req, res) => {
  const q = "DELETE FROM evento WHERE `id` = ?";

  db.query(q, [req.params.id], (err) => {
    if (err) return res.json(err);

    return res.status(200).json("Evento deletado com sucesso.");
  });
};