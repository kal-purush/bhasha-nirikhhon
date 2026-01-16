export class ExceptionHandler extends Error {
  constructor(message: string, public status_code: number) {
    super(`${message}`);
    this.message = message;
    this.status_code = status_code;
  }
}