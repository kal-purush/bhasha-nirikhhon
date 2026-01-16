import { Card } from "../cards/types";
import { ChooseCardAction, PlayerState, ThunkResult } from "../types";
import { waitForActionAction } from "./wait-for-action";

export const askForCardAction = (
	from: keyof PlayerState,
	cardType: typeof Card
): ThunkResult<Promise<ChooseCardAction>> => async (dispatch, getState) => {
	dispatch({ type: 'ask-for-card', from, cardType });
	return dispatch(waitForActionAction('choose-card'));
};