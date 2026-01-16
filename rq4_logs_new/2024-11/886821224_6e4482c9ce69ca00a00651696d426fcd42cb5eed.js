export const paginateResults = (items, page = 1, limit = 50) => {
  const startIndex = (page - 1) * limit;
  const endIndex = page * limit;

  const results = {
    items: items.slice(startIndex, endIndex),
    pagination: {
      currentPage: page,
      totalPages: Math.ceil(items.length / limit),
      totalItems: items.length,
      itemsPerPage: limit,
      hasNextPage: endIndex < items.length,
      hasPrevPage: page > 1
    }
  };

  return results;
}; 