import { Ricetta } from "../models/Ricetta";
import { RicettaArchiver } from "./RicettaArchiver";
export class RicettaController {
  constructor() {}

  // controllo lunghezza del codice ricetta
  public CheckLunghezzaCodice(codice: string) {
    if (codice.length === 15) {
      console.log("ok codice");
      return true;
    } else {
      console.log("ciaoooo");
      return false;
    }
  }
  // controllo regione
  public CheckRegione(regione: string, regioni: string[]) {
    const index = regioni.indexOf(regione);
    if (index >= 0) {
      console.log("ok regione");
      return true;
    } else {
      console.log("regione non esiste");
      return false;
    }
  }
  // controllo tipo di visita
  public CheckTipoVisita(tipo: string, visite: string[]) {
    const index = visite.indexOf(tipo);
    if (index >= 0) {
      console.log("ok tipo visita");
      return true;
    } else {
      console.log("tipo visita non esiste");
      return false;
    }
  }

  public CheckAll(
    codice: string,
    regione: string,
    regioni: string[],
    tipo: string,
    visite: string[]
  ) {
    if (this.CheckLunghezzaCodice(codice)) {
      if (this.CheckRegione(regione, regioni)) {
        if (this.CheckTipoVisita(tipo, visite)) {
          const ricetta = new RicettaArchiver();
          return ricetta.CreateRicetta(codice, tipo, regione);
        }
      }
    }
  }
}