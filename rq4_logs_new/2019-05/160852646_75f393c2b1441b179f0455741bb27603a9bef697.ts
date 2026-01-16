import { Converter } from "../../../common/Converter";
import { ICommentModel } from "../../models/Comment";
import { ICommentView } from "../../interface/supply/details/ICommentView";
import { UserModelToUserViewConverter } from "./UserModelToUserViewConverter";

export class CommentModelToCommentViewConverter implements Converter<ICommentModel, ICommentView> {
	convert(input: ICommentModel): ICommentView {
		const userConverter = new UserModelToUserViewConverter();

		return {
			content: input.content,
			date: input.date,
			author: userConverter.convert(input.author),
		};
	}
}