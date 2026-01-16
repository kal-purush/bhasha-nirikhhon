import { Request, RequestHandler, Response } from "express";
import { UserManager } from "../models";
import { User } from "../models/UserManager";
import * as argon2 from "argon2";
import { generateToken } from "../services/auth";

interface Error {
  message: string;
}

export default class UserController {
  static register: RequestHandler = async (req: Request, res: Response) => {
    const { email, password, role } = req.body;
    if (!email) {
      if (!password) {
        return res.status(500).send({
          error: "Email and password missing",
        });
      }
      return res.status(500).send({
        error: "Email missing",
      });
    }
    if (!password)
      return res.status(500).send({
        error: "Password missing",
      });

    Promise.all([UserManager.findByMail(email), argon2.hash(password)]).then(
      ([existingEmail, hashPassword]) => {
        console.log(existingEmail)
        if (existingEmail)
          return res.status(403).send({
            error: "Email already used",
          });
        UserManager.insert({ ...req.body, password: hashPassword })
          .then(([result]: any) => {
            res.status(201).send({
              id: result.insertId,
              email: email,
              role: role,
            });
          })
          .catch((err: Error) => {
            console.error(err);
            res.status(500).send({
              error: err.message,
            });
          });
      }
    );
  };

  // static login: RequestHandler = (req: Request, res: Response) => {
  //   const { email, password } = req.body;

  //   if (!email) {
  //     if (!password) {
  //       return res.status(500).send({
  //         error: "Email and password missing",
  //       });
  //     }
  //     return res.status(500).send({
  //       error: "Email missing",
  //     });
  //   }
  //   if (!password)
  //     return res.status(500).send({
  //       error: "Password missing",
  //     });

  //   UserManager.findByMail(email)
  //     .then((existingEmail) => {
  //       if (!existingEmail[0]) {
  //         return res.status(500).send({
  //           error: "Email isn't registered",
  //         });
  //       } else {
  //         const { id, email, password: hash, role_id } = existingEmail[0];
  //         argon2.verify(hash, password).then((passVerified: boolean) => {
  //           if (!passVerified)
  //             return res.status(403).send({
  //               error: "Wrong password",
  //             });
  //           const token = generateToken(existingEmail[0]);
  //           return res
  //             .cookie("access_token", token, {
  //               httpOnly: true,
  //               secure: process.env.NODE_ENV === "production",
  //             })
  //             .status(200)
  //             .send({
  //               id,
  //               email,
  //               role_id,
  //             });
  //         });
  //       }
  //     })
  //     .catch((err: Error) => {
  //       console.error(err);
  //       res.status(500).send({
  //         error: err.message,
  //       });
  //     });
  // };

  static browse: RequestHandler = (req: Request, res: Response) => {
    UserManager.findAll()
      .then(rows => {
        res.status(201).send(
          rows
          // .map((person) => ({
          //   id: person.id,
          //   email: person.email,
          //   role: person.role_id,
          // }))
        );
      })
      .catch((err: Error) => {
        console.error(err);
        res.status(500).send({
          error: err.message,
        });
      });
  };

  static logout: RequestHandler = (req: Request, res: Response) => {
    return res.clearCookie("access_token").sendStatus(200);
  };

  static edit = (req: Request, res: Response) => {
    const user = req.body;

    user.id = parseInt(req.params.id, 10);

    UserManager.update(user)
      .then(([result]: any) => {
        if (result.affectedRows === 0) {
          res.sendStatus(404);
        } else {
          res.sendStatus(204);
        }
      })
      .catch((err: Error) => {
        console.error(err);
        res.sendStatus(500);
      });
  };

  static delete: RequestHandler = (req: Request, res: Response) => {
    UserManager.delete(parseInt(req.params.id, 10))
      .then(() => {
        res.sendStatus(204);
      })
      .catch((err: Error) => {
        console.error(err);
        res.sendStatus(500);
      });
  };
}