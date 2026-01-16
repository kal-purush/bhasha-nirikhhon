import * as debug from "debug";
import * as express from "express";
import { PrenotazioniController } from "../controllers/PrenotazioniController";
import { Patient } from "../models/Patient";
import { Visita } from "../models/Visita";

const logger = debug("app:startup");
const router = express.Router();
router.use(express.json());
router.get("/", (req, res) => res.send("Buru world"));

router.post("/annullaVisita", (req, res) => {
  let newRep: number | undefined;
  newRep = PrenotazioniController.annullaPrenotazione(JSON.stringify(req.body));
  if (newRep !== undefined) {
    res.send(newRep.toString());
  } else {
    res.send("broken");
  }
});

logger(
  JSON.stringify(
    new Visita("aaa", false, 2, false, new Date("12/2/2019"), new Patient(8))
  )
);
export = router;