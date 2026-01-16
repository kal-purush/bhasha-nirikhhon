import { Converter } from "../../../common/Converter";
import { IDayOffModel } from "../../models/DayOff";
import { IDayOffDetails } from "../../interface/worker/details/IDayOffDetails";
import { UserModelToUserViewConverter } from "../common/UserModelToUserViewConverter";
import { IUserModel } from "../../models/user";

export class DayOffModelToDayOffDateilsConverter implements Converter<IDayOffModel, IDayOffDetails> {
	convert(input: IDayOffModel): IDayOffDetails {
		const userConverter = new UserModelToUserViewConverter();

		return {
			id: input._id,
			date: input.date,
			state: input.state,
			resolvedBy: input.resolvedBy ? userConverter.convert(<IUserModel>input.resolvedBy.person) : undefined,
			resolvedDate: input.resolvedDate,
		}
	}
}