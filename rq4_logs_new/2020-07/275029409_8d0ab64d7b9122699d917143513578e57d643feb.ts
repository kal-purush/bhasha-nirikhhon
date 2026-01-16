import knex from "../database/connection";

interface Point {
  image: string;
  name: string;
  email: string;
  whatsapp: string;
  latitude: number;
  longitude: number;
  city: string;
  uf: string;
  items: string;
}

export default class PointService {
  static async create(pointInput: Point) {
    try {
      const { items, ...pointData } = pointInput;
      const trx = await knex.transaction();

      const point = {
        ...pointData,
      };

      const existingPoint = await trx("points")
        .where("name", pointData.name)
        .first();
      if (existingPoint) {
        throw new Error("Point Already exists");
      }

      const [id] = await trx("points").returning("id").insert(point);

      const pointItems = items
        .split(",")
        .map((item: string) => Number(item.trim()))
        .map((item_id: number) => {
          return {
            item_id,
            point_id: id,
          };
        });
      await trx("point_items").insert(pointItems);
      await trx.commit();

      return { id, point };
    } catch (error) {
      console.error(error);
      throw new Error(error.message);
    }
  }

  static async show(id: number) {
    try {
      const point = await knex("points").where("id", id).first();
      const items = await knex("items")
        .join("point_items", "items.id", "=", "point_items.item_id")
        .where("point_items.point_id", id)
        .select("items.title");
      return { point, items };
    } catch (error) {
      throw new Error(error.message);
    }
  }

  static async index(city: string, uf: string, parsedItems: number[]) {
    try {
      const points = await knex("points")
        .join("point_items", "points.id", "=", "point_items.point_id")
        .whereIn("point_items.item_id", parsedItems)
        .where("city", city)
        .where("uf", uf)
        .distinct()
        .select("points.*");
      return points;
    } catch (error) {
      throw new Error(error.message);
    }
  }

  static async update(id: number, pointInput: any) {
    try {
      const existingPoint = await knex("points").where("id", id).first();

      if (!existingPoint) {
        throw new Error("This point does not exist exists");
      }

      const point = await knex("points")
        .where("id", id)
        .update({ ...pointInput });

      console.log(point);

      return point;
    } catch (error) {
      console.error(error);
      throw new Error(error.message);
    }
  }

  static async destroy(id: number) {
    try {
      const existingPoint = await knex("points").where("id", id).first();
      if (!existingPoint) {
        throw new Error("Point not found");
      }
      await knex("points").where("id", id).delete();
    } catch (error) {
      console.error(error);
      throw new Error(error.message);
    }
  }
}