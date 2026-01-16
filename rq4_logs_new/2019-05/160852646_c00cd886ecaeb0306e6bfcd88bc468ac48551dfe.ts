import { Error as MongooseError } from "mongoose";
import { IMenuModel, Menu } from "../../models/menu";

export class MenuRepository {
	async changeName(id: string, newName: string): Promise<void> {
		let document: IMenuModel | null;

		try {
			document = await Menu.findByIdAndUpdate(id, { $set: { name: newName } }).exec();
		} catch (err) {
			if (err instanceof MongooseError.CastError) {
				throw new InvalidObjectIdError(id);
			}
			throw err;
		}

		if (!document) {
			throw new MenuNotFoundError(id);
		}
	}
}

export class MenuNotFoundError extends Error {
	constructor(id: string) {
		super(`Menu with ID: ${id} was not found`);
	}
}

export class InvalidObjectIdError extends Error {
	constructor(id: string) {
		super(`"${id}" is not a valid object id`);
	}
}