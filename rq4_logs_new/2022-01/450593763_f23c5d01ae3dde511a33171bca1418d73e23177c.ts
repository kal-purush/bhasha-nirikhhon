import {GET_CARDS, GET_CARD, ADD_CARD, REMOVE_CARD, CARDS_ERROR, CLEAR_CARDS, UPDATE_DEFAULT_CARD} from './types';
import customAxios from '../utils/customAxios';
import {AxiosResponse} from 'axios';
import {CreditCard, CreditCardsOptions} from '../types/redux';
import {clientQueryOptions} from '../utils/clientQueryOptions';
import {CreditCardForm} from '../types/form';

export const getCards = (options?: CreditCardsOptions | undefined) => async (dispatch: any) => {
	try {
		const res: AxiosResponse<{cards: CreditCard[]}> = await customAxios.get(`/api/v1/finance/secure/cards${options ? clientQueryOptions(options) : ''}`);
		dispatch({
			type: GET_CARDS,
			payload: res.data.cards
		});
	} catch (err) {
		console.error(err);
		dispatch({
			type: CARDS_ERROR,
			payload: err
		});
	}
};

export const getCard = (cardId: number) => async (dispatch: any) => {
	try {
		const res: AxiosResponse<{card: CreditCard}> = await customAxios.get(`/api/v1/finance/secure/cards/${cardId}`);
		dispatch({
			type: GET_CARD,
			payload: res.data.card
		});
	} catch (err) {
		console.error(err);
		dispatch({
			type: CARDS_ERROR,
			payload: err
		});
	}
};

export const addNewCard = (formData: CreditCardForm) => async (dispatch: any) => {
	console.log('hey dude');
	const config = {
		headers: {
			'Content-Type': 'application/json'
		}
	};
	const body = JSON.stringify(formData);
	console.log({body});
	try {
		const res: AxiosResponse<{data: string}> = await customAxios.post(`/api/v1/finance/secure/new-card`, body, config);
		dispatch({
			type: ADD_CARD,
			payload: res.data.data
		});
	} catch (err) {
		console.error(err);
		dispatch({
			type: CARDS_ERROR,
			payload: err
		});
	}
};

export const updateDefaultCard = (cardId: number) => async (dispatch: any) => {
	try {
		await customAxios.put(`/api/v1/finance/secure/cards/default/${cardId}`);
		dispatch({
			type: UPDATE_DEFAULT_CARD,
			cardId
		});
	} catch (err) {
		console.error(err);
		dispatch({
			type: CARDS_ERROR,
			payload: err
		});
	}
};

export const deleteCard = (cardId: number) => async (dispatch: any) => {
	try {
		const res: AxiosResponse<{data: string}> = await customAxios.delete(`/api/v1/finance/secure/delete/${cardId}`);
		dispatch({
			type: REMOVE_CARD,
			payload: res.data.data,
			cardId
		});
	} catch (err) {
		console.error(err);
		dispatch({
			type: REMOVE_CARD,
			payload: err
		});
	}
};

export const clearCards = () => (dispatch: any) => {
	dispatch({
		type: CLEAR_CARDS
	});
};