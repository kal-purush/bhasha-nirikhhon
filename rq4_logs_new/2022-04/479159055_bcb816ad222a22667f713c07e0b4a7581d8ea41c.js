import { SET_FILTER } from "./filter.const";

// Принимает имя фильтра и возвращает объект action
export const setFilter = (filter) => ({
	type: SET_FILTER,
	filter
})