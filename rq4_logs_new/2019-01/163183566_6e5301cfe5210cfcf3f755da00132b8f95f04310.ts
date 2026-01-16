import { Request, Response } from "express";

const Twit = require("../../models").Twit;
const User = require("../../models").User;

export class TweetController {
  public async All(req: Request, res: Response) {
    try {
      const Tweets = await Twit.findAll({
        attributes: ["id", "text", "createdAt"],
        include: [
          { model: User, as: "author", attributes: ["name", "username"] }
        ],
        order: [["createdAt", "DESC"]]
      });
      return res.send(Tweets);
    } catch (error) {
      return res.send(error.message);
    }
  }

  public async Save(req: Request, res: Response) {
    try {
      const authorId = req["user"].id;
      const text = req.body.text.trim();

      if (!text) {
        return res.status(400).send({ message: "A tweet must have a body" });
      }

      const Tweet = await Twit.create({
        text,
        user_id: authorId
      });

      return res.status(201).send(Tweet);
    } catch (error) {
      return res.send({
        message: "An error occurred while creating this twit",
        error: error.message
      });
    }
  }
}