import { Visita } from "./Visita";

export class Notifica {
  constructor(
    private _testo: string,
    private _visita: Visita,
    private _tipo: string
  ) {}

  public get testo() {
    return this._testo;
  }

  public set testo(t: string) {
    this._testo = t;
  }

  public get visita() {
    return this._visita;
  }

  public set visita(v: Visita) {
    this._visita = v;
  }

  public get tipo() {
    return this._tipo;
  }

  public set tipo(type: string) {
    this._tipo = type;
  }
}