export function paginate(items, currentPage, perPageItems) {
	let page = currentPage || 1,
		perPage = perPageItems || 3,
		offset = (page - 1) * perPage,
		paginatedItems = items.slice(offset).slice(0, perPageItems),
		totalPages = Math.ceil(items.length / perPage);

	return {
		page: page,
		perPage: perPage,
		prePage: page - 1 ? page - 1 : null,
		nextPage: totalPages > page ? page + 1 : null,
		total: items.length,
		totalPages: totalPages,
		data: paginatedItems
	};
}