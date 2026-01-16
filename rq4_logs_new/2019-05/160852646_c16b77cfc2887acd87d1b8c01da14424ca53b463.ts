import { Converter } from "../../../common/Converter";
import { ISupplyModel } from "../../models/Supply";
import { ISupplyView } from "../../interface/supply/list/ISupplyView";

export class SupplyModelToSupplyViewConverter implements Converter<ISupplyModel, ISupplyView> {
	convert(input: ISupplyModel): ISupplyView {
		const person = input.requestedBy;

		return {
			id: input._id,
			name: input.name,
			price: input.price,
			requestedBy: `${person.firstName} ${person.lastName}`,
			requestedDate: input.requestedDate,
			state: input.currentState.state,
			url: input.url,
		};
	}
}