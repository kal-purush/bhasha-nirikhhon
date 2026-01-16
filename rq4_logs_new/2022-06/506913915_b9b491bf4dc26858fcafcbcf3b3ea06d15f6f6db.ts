import Joi from "joi";
import AbstractManager from "./AbstractManager";

interface Role {
  id: number;
  role_name: string;
}

export default class RoleManager extends AbstractManager {
  static table = "role";

  static async insert(role: Role) {
    return (await this.connection).query(
      `INSERT INTO ${RoleManager.table} SET ?`,
      [role]
    );
  }

  static async validate(data: Role, forCreation: boolean = true) {
    const presence = forCreation ? "required" : "optional";
    return await Joi.object({
      role_name: Joi.string().presence(presence),
    }).validate(data, { abortEarly: false }).error;
  }
}