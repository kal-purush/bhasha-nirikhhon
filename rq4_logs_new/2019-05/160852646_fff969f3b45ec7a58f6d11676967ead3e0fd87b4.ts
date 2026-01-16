import { NextFunction, Response } from "express";
import { IRequest } from "../../models/Express";
import { User } from "../models/user";
import { Price } from "../models/Price";
import { ISupplyCreateRequest } from "../interface/supply/create/ISupplyCreateRequest";
import { SupplyRepository } from "./SupplyRepository";
import { ISupply } from "../../interface/Supply";

export class SupplyController {
	private repository = new SupplyRepository();

	async newSupplyRequest(req: IRequest, res: Response, next: NextFunction): Promise<Response> {
		const request: ISupplyCreateRequest = req.body;

		const user = await User.findById(req.context!.userId).exec();
		const model: ISupply = {
			name: request.name,
			price: new Price(request.price),
			requestedBy: user!,
			description: request.description,
			url: request.url,
		};

		try {
			await this.repository.save(model);
		} catch (err) {
			console.log(err);
			return res.status(400).json();
		}

		return res.status(201).json();
	}
}