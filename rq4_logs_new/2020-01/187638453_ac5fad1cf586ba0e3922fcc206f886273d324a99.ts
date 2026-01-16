import { loremIpsum } from 'lorem-ipsum';

import { CTAProps } from '../../components/CTA/CTA';
import { BUTTONS_MOCK } from '../BlockButtons/BlockButtons.mock';

const content = loremIpsum({ count: 10 });

export const CTAS_MOCK: CTAProps[] = [
	{
		content,
		heading: 'CTA 1',
		headingType: 'h2',
		button: BUTTONS_MOCK[0],
	},
	{
		content,
		heading: 'CTA 2',
		headingType: 'h2',
		button: BUTTONS_MOCK[0],
	},
];