type GetPageContextsParams = {
	totalCount: number;
	limit: number;
};

export const getPagesContext = ({
	totalCount,
	limit,
}: GetPageContextsParams) => {
	const totalPagesCount = Math.ceil(totalCount / limit);

	return new Array(totalPagesCount).fill("").map((_, i) => {
		const offset = limit * i;
		const currentPageNum = (offset + limit) / limit;

		return {
			limit,
			offset,
			totalCount,
			currentPageNum,
			totalPagesCount,
		} as const;
	});
};