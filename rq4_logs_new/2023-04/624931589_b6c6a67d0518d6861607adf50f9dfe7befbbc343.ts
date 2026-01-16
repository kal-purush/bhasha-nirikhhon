// External Dependencies
import express, { Request, Response } from "express";
import { ObjectId } from "mongodb";
import { collections } from "../utils/database.service";
import Project from "../interfaces/Project";

// Global Config
export const projectRouter = express.Router();

projectRouter.use(express.json());

// GET
projectRouter.get("/", async (_req: Request, res: Response) => {
  console.log("im here!!");
  try {
    const projects = (await collections.projects
      ?.find({})
      .toArray()) as unknown as Project[];

    res.status(200).send(projects);
  } catch (error) {
    res.status(500).send(error);
  }
});

// POST
projectRouter.post("/", async (req: Request, res: Response) => {
  try {
    const newGame = req.body as Project;
    const result = await collections.projects?.insertOne(newGame);

    result
      ? res
          .status(201)
          .send(`Successfully created a new game with id ${result.insertedId}`)
      : res.status(500).send("Failed to create a new game.");
  } catch (error) {
    console.error(error);
    res.status(400).send(error);
  }
});

// PUT

// DELETE