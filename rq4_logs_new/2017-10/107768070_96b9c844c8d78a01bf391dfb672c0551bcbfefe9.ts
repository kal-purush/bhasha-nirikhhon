export class Item {
  public id: number;
  private _text: string;
  private _status: boolean;

  get text(): string {
    return this._text;
  }

  get status(): boolean {
    return this._status;
  }

  set text(value: string) {
    this._text = value;
  }

  set status(value: boolean) {
    this._status = value;
  }

  changeStatus(): void {
    if (this.status)
      this.status = false;
    else
      this.status = true;
  }
  constructor(data?: any){
    if (data){
      this.id = data.id;
      this.text = data._text;
      this.status = data._status;
    }
  }
}