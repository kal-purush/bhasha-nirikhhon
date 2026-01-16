export class User {
  private id: Number;
  private username: String;
  private password: String;
  private roleId: Number;
  private departmentId: boolean;
  constructor(
    id: Number,
    username: String,
    password: String,
    roleId: Number,
    departmentId: boolean,
  ) {
    this.id = id;
    this.username = username;
    this.password = password;
    this.roleId = roleId;
    this.departmentId = departmentId;
  }
  getId() {
    return this.id;
  }
}