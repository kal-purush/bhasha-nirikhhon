import { Referto } from "./Referto";

export class Visita {
  constructor(
    private _tipoVisita: string,
    private _effettuata: boolean,
    private _pagata: boolean,
    private _data: Date,
    private _referto: Referto
  ) {}

  public get tipoVisita() {
    return this._tipoVisita;
  }

  public set tipoVisita(tv: string) {
    this._tipoVisita = tv;
  }

  public get effettuata() {
    return this._effettuata;
  }

  public set effettutata(eff: boolean) {
    this._effettuata = eff;
  }

  public get pagata() {
    return this._pagata;
  }

  public set pagata(pag: boolean) {
    this._pagata = pag;
  }
  public get data() {
    return this._data;
  }

  public set data(dt: Date) {
    this._data = dt;
  }
  public get referto() {
    return this._referto;
  }

  public set referto(rf: Referto) {
    this._referto = rf;
  }
}