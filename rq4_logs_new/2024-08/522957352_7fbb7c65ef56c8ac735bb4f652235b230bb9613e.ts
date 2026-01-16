import { Schema } from '@fincity/kirun-js';
import {
	SCHEMA_ANY_COMP_PROP,
	SCHEMA_BOOL_COMP_PROP,
	SCHEMA_STRING_COMP_PROP,
	SCHEMA_NUM_COMP_PROP,
} from '../../constants';
import {
	ComponentPropertyEditor,
	ComponentPropertyDefinition,
	ComponentStylePropertyGroupDefinition,
	ComponentStylePropertyDefinition,
	ComponentPropertyGroup,
} from '../../types/common';
import { COMMON_COMPONENT_PROPERTIES, COMPONENT_STYLE_GROUP_PROPERTIES } from '../util/properties';
const propertiesDefinition: Array<ComponentPropertyDefinition> = [
	{
		name: 'timerType',
		schema: SCHEMA_STRING_COMP_PROP,
		displayName: 'Timer Type',
		description: 'Choose between Repeating or Non-Repeating',
		group: ComponentPropertyGroup.BASIC,
		defaultValue: 'Non-Reapeating',
		editor: ComponentPropertyEditor.ENUM,
		enumValues: [
			{ name: 'Reapeating', displayName: 'Repeating' },
			{ name: 'Non-Reapeating', displayName: 'Non-Repeating' },
		],
	},
	{
		name: 'duration',
		schema: SCHEMA_NUM_COMP_PROP,
		displayName: 'Duration (ms)',
		description: 'Duration of the timer in milliseconds',
		group: ComponentPropertyGroup.DATA,
		defaultValue: 5000,
	},
	{
		name: 'initialDelay',
		schema: SCHEMA_NUM_COMP_PROP,
		displayName: 'Initial Delay (ms)',
		description: 'Delay before the first execution of the timer',
		group: ComponentPropertyGroup.DATA,
		defaultValue: 1000,
	},
	{
		name: 'repeatCount',
		schema: SCHEMA_NUM_COMP_PROP,
		displayName: 'Repeat Count',
		description: 'Number of times to repeat the timer (-1 for indefinite)',
		group: ComponentPropertyGroup.DATA,
		defaultValue: -1,
	},
	{
		name: 'onTimerEventFunction',
		schema: SCHEMA_STRING_COMP_PROP,
		displayName: 'OnTimer Event Function',
		description: 'Function to execute on each timer event',
		group: ComponentPropertyGroup.EVENTS,
		editor: ComponentPropertyEditor.EVENT_SELECTOR,
	},
	COMMON_COMPONENT_PROPERTIES.visibility,
];

const stylePropertiesDefinition: ComponentStylePropertyDefinition = {
	'': [],
};

export { propertiesDefinition, stylePropertiesDefinition };