export class AutoMessageTemplate {

  private _messageId: number;
  private _messageText: string;


  constructor(messageId: number, messageText: string) {
    this._messageId = messageId;
    this._messageText = messageText;
  }

  get messageId() {
    return this._messageId;
  }

  set messageId(value) {
    this._messageId = value;
  }

  get messageText() {
    return this._messageText;
  }

  set messageText(value) {
    this._messageText = value;
  }
}