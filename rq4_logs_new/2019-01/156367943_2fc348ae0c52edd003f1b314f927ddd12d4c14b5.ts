import { Assenza } from "../models/Assenza";
import { Notificator } from "../models/Notificator";
import { OfficeDoctor } from "../models/OfficeDoctor";
import { Visita } from "../models/Visita";
import { VisitaGetter } from "../models/VisitaGetter";

export class AssenzaController {
  public creaAssenza(
    _intervallo: Date[],
    _motivazione: string = "",
    _officeDoctor: OfficeDoctor
  ) {
    if (this.checkIntervallo(_intervallo) === true) {
      const assenza = new Assenza(_intervallo, _motivazione, _officeDoctor);
      let visite: Visita[] = [];

      for (let i = 0; i < _intervallo.length; i++) {
        const vGetter = new VisitaGetter();
        visite = vGetter.getVisite(_intervallo[i], _officeDoctor);

        for (let j = 0; j < visite.length; j++) {
          const not = new Notificator();
          not.creaNotifica(
            "Dottore Assente",
            visite[j],
            "Notofica Assenza",
            assenza
          );
        }
      }

      return true;
    }
    return false;
  }

  public checkIntervallo(interv: Date[]) {
    if (interv[0] <= interv[1]) {
      return true;
    }

    return false;
  }
}