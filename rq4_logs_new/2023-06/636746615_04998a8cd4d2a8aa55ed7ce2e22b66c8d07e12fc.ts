import type { Meta, StoryObj } from '@storybook/web-components';
import { html } from 'lit';

import './ms-input-checkbox';

export default {
    title: 'Checkbox',
    argTypes: {
        onCheck: { action: 'onChange' },
    },
    render: (args) => html`
    <ms-input-checkbox .title=${args.title} .description=${args.description}>${args.label}</ms-input-checkbox>
    `
} as Meta;

export const InputCheckbox: StoryObj = {
    name: 'MSInputCheckbox',
    args: {
        title: 'Title',
        description: 'Description',
    }
}