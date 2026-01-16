export class User {
  private _email: string;
  private _firstName: string;
  private _gender: string;
  private _id: number;
  private _lastName: string;

  constructor(
    id: number,
    firstName: string,
    lastName: string,
    email: string,
    gender: string
  ) {
    this._id = id;
    this._firstName = firstName;
    this._lastName = lastName;
    this._gender = gender;
    this._email = email;
  }

  public get email(): string {
    return this._email;
  }
  public set email(value: string) {
    this._email = value;
  }

  public get firstName(): string {
    return this._firstName;
  }
  public set firstName(value: string) {
    this._firstName = value;
  }

  public get gender(): string {
    return this._gender;
  }
  public set gender(value: string) {
    this._gender = value;
  }

  public get id(): number {
    return this._id;
  }

  public get lastName(): string {
    return this._lastName;
  }
  public set lastName(value: string) {
    this._lastName = value;
  }
}