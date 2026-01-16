export class LocalStorage {

  private readonly key: string;

  constructor(key: string) {
    this.key = key;
  }

  add(objects) {
    localStorage.setItem(this.key, JSON.stringify(objects));
  }

  read() {
    return JSON.parse(localStorage.getItem(this.key));
  }

  readWithCheck() {
    return JSON.parse(localStorage.getItem(this.key)) == null;
  }
}