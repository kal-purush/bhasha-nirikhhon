import { Allegato } from "../models/Allegato";
import { Notifica } from "../models/Notifica";
import { Notificator } from "../models/Notificator";
import { Referto } from "../models/Referto";
import { Visita } from "../models/Visita";
import { ControllerVisite } from "./ControllerVisite";

let notifica: Notifica;

export class RefertoManager {
  public aggiungiReferto(
    nome: string,
    path: string,
    allegati: Allegato[],
    visita: Visita
  ) {
    const referto = new Referto(nome, path, allegati);

    const caricato = ControllerVisite.addReferto(visita, referto);

    if (caricato === true) {
      const notificator = new Notificator();
      notifica = notificator.creaNotifica(
        "Referto Caricato",
        visita,
        "refertoCaricato"
      );
    }

    return notifica;
  }
}