import type { Meta, StoryObj } from '@storybook/web-components';
import { html } from 'lit';

import './ms-checkbox';

export default {
    title: 'Checkbox',
    argTypes: {
        onCheck: { action: 'onChange' },
    },
    render: (args) => html`
    <ms-checkbox .checked=${args.checked} @change=${args.onCheck} .title=${args.title} ?disabled=${args.disabled} .description=${args.description}>${args.label}</ms-checkbox>
    `
} as Meta;

export const Checkbox: StoryObj = {
    name: 'MSCheckbox',
    args: {
        title: '',
        description: '',
        checked: false,
        disabled: true
    }
}