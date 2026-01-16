import { Request, Response } from "express";
import db from "../database/connection";
import convertHourToMinutes from "../utils/convert";
import { CONNECTIONS_TABLE } from "../database/constantsDatabase";

export default class ConnectionsControllers {
  async index(request: Request, response: Response) {
    try {
      const totalConnections = await db(CONNECTIONS_TABLE.TABLE_NAME).count(
        "* as total"
      );

      const { total } = totalConnections[0];

      return response.status(201).json({ total });
    } catch (error) {
      return response.status(400).json({
        mensage: "Unexpected error while get connection",
        error,
      });
    }
  }

  async create(request: Request, response: Response) {
    const { user_id } = request.body;
    try {
      await db(CONNECTIONS_TABLE.TABLE_NAME).insert({ user_id });
      return response.status(201).json({ mensage: "created" });
    } catch (error) {
      return response.status(400).json({
        mensage: "Unexpected error while create connection",
        error,
      });
    }
  }
}