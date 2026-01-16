import { Ficha } from "./ficha.model";
import { Injectable } from "@angular/core";
import { Subject } from "rxjs";

@Injectable({ providedIn: "root" })
export class FichasService {
  private fichas: Ficha[] = [];
  private fichasUpdated = new Subject<Ficha[]>();

  getFichas() {
    return [...this.fichas];
  }

  getFichasUpdateListener() {
    return this.fichasUpdated.asObservable();
  }

  addFicha(
    nome: string,
    matricula: string,
    leito: string,
    data: string,
    percepSens: any,
    umidade: any,
    atividade: any,
    mobilidade: any,
    nutricao: any,
    fricscisal: any,
    score: any
  ) {
    const ficha: Ficha = {
      nome: nome,
      matricula: matricula,
      leito: leito,
      data: data,
      percepSens: percepSens,
      umidade: umidade,
      atividade: atividade,
      mobilidade: mobilidade,
      nutricao: nutricao,
      fricscisal: fricscisal,
      score: score
    };
    this.fichas.push(ficha);
    this.fichasUpdated.next([...this.fichas]);
  }
}