import React from 'react';
import { IconType } from 'react-icons/lib';

export type ElementsType = 'text';
export interface FormElement {
	type: ElementsType;
	construct: (id: string) => FormElementInstance;
	designerButtonElement: {
		icon: IconType;
		label: string;
	};
	designerComponent: React.FC<{ elementInstance: FormElementInstance }>;
	formComponent: React.FC;
	propertiesComponent: React.FC;
}
export interface FormElementInstance {
	id: string;
	type: ElementsType;
	extraAttributes?: Record<string, unknown>;
}
export type FormElementsType = {
	[key in ElementsType]: FormElement;
};