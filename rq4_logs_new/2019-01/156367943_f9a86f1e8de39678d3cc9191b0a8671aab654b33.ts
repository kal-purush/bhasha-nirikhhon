export class Allegato {
  constructor(private _nome: string, private _path: string) {}
  public get nome() {
    return this._nome;
  }
  public get path() {
    return this._path;
  }

  public equals(obj: object): boolean {
    if (obj === null) {
      return false;
    }
    if (obj === this) {
      return true;
    }
    if (!(obj instanceof Allegato)) {
      return false;
    }
    const allegato = obj as Allegato;
    if (this.nome === allegato.nome && this.path === allegato.path) {
      return true;
    }
    return false;
  }
}