import prenotazioni from "../data/prenotazioni";
import OfficeDoctor from "../models/OfficeDoctor";
import Patient from "../models/Patient";
import Prenotazione from "../models/Prenotazione";

export default abstract class PrenotazioniGetter {
  public static getPrenotazioniDottore(
    data: Date,
    officeDoctor: OfficeDoctor
  ): Prenotazione[] {
    throw new Error("Method not implemented.");
  }
  public static getPrenotazioniPaziente(
    date: Date[],
    paziente: Patient
  ): Prenotazione[] {
    throw new Error("Method not implemented.");
  }
  public static ottieniListaPrenotazioni(
    prenotazione: Prenotazione,
    dataInizio: Date
  ): Prenotazione[] {
    return prenotazioni.filter(
      (p) =>
        p.visita.pagata === false &&
        p.visita.effettuata === false &&
        p.visita.tipoVisita === prenotazione.visita.tipoVisita &&
        p.data >= dataInizio &&
        !p.equals(prenotazione)
    );
  }
}