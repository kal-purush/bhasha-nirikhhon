import { addAuthorWithApi, saveAuthorsWithApi } from './../../services';
import { addNewAuthorAction, saveAuthorsAction } from './actions';

export const saveAuthorsThunk = () => {
	return async function (dispatch) {
		saveAuthorsWithApi()
			.then((authors) => dispatch(saveAuthorsAction(authors)))
			.catch((err) => console.log('Error saving authors:', err));
	};
};

export const addAuthorThunk = (name: string) => {
	return async function (dispatch) {
		addAuthorWithApi(name)
			.then((author) => {
				if (author) {
					dispatch(addNewAuthorAction(author));
				}
			})
			.catch((err) => console.log('Error adding author:', err));
	};
};