import {ACTION} from '../Constants/Actions';

export interface ISliderAction {
	type: ACTION;
	id: number;
	value: number;
}

export function sliderChange(id: number, value: number): ISliderAction {
	return {
		type: ACTION.SliderChange,
		id,
		value,
	};
}
