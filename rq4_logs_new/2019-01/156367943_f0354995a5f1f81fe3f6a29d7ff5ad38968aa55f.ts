import * as express from "express";
import { RefertoManager } from "../controllers/RefertoManager";
import { Allegato } from "../models/Allegato";
import { Referto } from "../models/Referto";
import { Visita } from "../models/Visita";

const router = express.Router();

const al1 = new Allegato("a", "x");
const al2 = new Allegato("b", "y");
const al3 = new Allegato("c", "z");

const al: Allegato[] = [];
al.push(al1, al2, al3);

const referto = new Referto("", "", al);
const visita = new Visita("Cardiologia", false, false, new Date(), referto);

const manager = new RefertoManager();

router.put("/", (req, res) => {
  const notifica = manager.aggiungiReferto("Ref01", "ciao", al, visita);

  if (notifica !== undefined) {
    res.send(notifica.testo);
  }

  res.send("OK");
});

router.get("/", (req, res) => res.send("done"));

export = router;