import { Router } from "express";
import { getPolls, getPollById, getByEvent, getResult, createPoll, vote, deletePoll, deleteAllPolls} from "../controllers/polls.controller"
import { checkJwt } from "../middlewares/authz.middleware.ts";

const router = Router();

const pathbase = '/api/v1/polls';

router.get(`${pathbase}`, getPolls)
router.get(`${pathbase}/:id`, getPollById)
router.get(`${pathbase}/getByEvent/:eventId`, getByEvent)
router.get(`${pathbase}/GetResult/:id`, getResult)
router.post(`${pathbase}`,[checkJwt], createPoll)
router.delete(`${pathbase}/:id`,[checkJwt], deletePoll)
router.delete(`${pathbase}`,[checkJwt], deleteAllPolls)
router.patch(`${pathbase}/vote/:optionId`, vote)

export default router;