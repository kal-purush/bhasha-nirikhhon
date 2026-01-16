import { Request, Response, Router } from "express";
import { Event } from "../../models/event.model";
import { User } from "../../models/user.model";
import EventService from "../../services/event/event.service";

const router = Router();

router.post("/", createEvent);

export function createEvent(req: Request, res: Response) {
  const user = req.user as User;
  console.log("user: ", req.body, " user2: ", user);
  EventService.createEvent(user, req.body as Event)
    .then((event: Event) => {
      res.status(200).json(event);
    })
    .catch((error: Error) => {
      console.log("err: ", error);
      res.status(500).send(error);
    });
}

export default router;