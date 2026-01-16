import { Router, Request, Response } from "express";
import { authenticate } from "../auth/auth";
import { SERVER_ERROR } from "../utils/errors";
import { validateRequest } from "../utils/validator";
import { addTest, addTestSubmission, getAllTests } from "./tests-service";

async function addTestHandler(req: Request, res: Response) {
  try {
    const testData = req.body;
    const classid = req.params.classid;
    await addTest(testData, classid);
    res.status(200).send();
  } catch (err) {
    if (err.code) res.status(err.code).json(err.message);
    else res.status(500).json(SERVER_ERROR);
  }
}
async function addTestSubmissionsHandler(req: Request, res: Response) {
  try {
    // await addTestSubmission(classroom);
    res.status(200).send();
  } catch (err) {
    if (err.code) res.status(err.code).json(err.message);
    else res.status(500).json(SERVER_ERROR);
  }
}
async function getAllTestsHandler(req: Request, res: Response) {
  try {
    // await getAllTests(classroom);
    res.status(200).send();
  } catch (err) {
    if (err.code) res.status(err.code).json(err.message);
    else res.status(500).json(SERVER_ERROR);
  }
}

export default function testController() {
  const router = Router();
  router.post("/:classid/new", authenticate(), addTestHandler);
  router.post("/submission", authenticate(), addTestSubmissionsHandler);
  router.get("/", authenticate(), getAllTestsHandler);
  return router;
}