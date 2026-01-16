import { Router, Request, Response } from "express";
import { authenticate, getUser } from "../auth/auth";
import { SERVER_ERROR } from "../utils/errors";
import { validateRequest } from "../utils/validator";
import { classSchema } from "./class-model";
import { createClass, deleteClass } from "./class-service";

export async function addClassHandler(req: Request, res: Response) {
  const user = res.locals.user;
  const classroom = req.body;
  try {
    await createClass(classroom, user);
    res.status(200).send();
  } catch (err) {
    if (err.code) res.status(err.code).json(err.message);
    else res.status(500).json(SERVER_ERROR);
  }
}
export async function deleteClassHandler(req: Request, res: Response) {
  const classid = req.params.classid;
  const user = res.locals.user;
  try {
    await deleteClass(classid, user);
    res.status(200).send();
  } catch (err) {
    console.log(err);
    if (err.code) res.status(err.code).json(err.message);
    else res.status(500).json(SERVER_ERROR);
  }
}

export default function classController() {
  const router = Router();
  router.post(
    "/new",
    authenticate(),
    validateRequest("body", classSchema),
    addClassHandler
  );
  router.delete("/remove/:classid", authenticate(), deleteClassHandler);
  return router;
}