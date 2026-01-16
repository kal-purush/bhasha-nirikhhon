import { Allegato } from "./Allegato";

export class Referto {
  constructor(
    private _nome: string,
    private _path: string,
    private _allegati: Allegato[]
  ) {}

  public get nome() {
    return this._nome;
  }

  public set nome(n: string) {
    this._nome = n;
  }

  public get path() {
    return this._path;
  }

  public set path(p: string) {
    this._path = p;
  }

  public get allegati() {
    return this._allegati;
  }

  public set allegati(a: Allegato[]) {
    this.allegati = a;
  }
}