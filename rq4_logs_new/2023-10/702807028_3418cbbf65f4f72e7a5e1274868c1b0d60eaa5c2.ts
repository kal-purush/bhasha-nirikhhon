import type { Meta, StoryObj } from '@storybook/react';

import { BasicModal } from './BasicModal';
import { userEvent, within } from '@storybook/testing-library';

const meta = {
    title: 'Components/Modal',
    component: BasicModal,
    parameters: {
        layout: 'centered',
    },

    tags: ['autodocs'],
    argTypes: {
        textHeader: { control: 'text' },
        title: { control: 'text' },
        description: { control: 'text' },
    },
} satisfies Meta<typeof BasicModal>;

export default meta;
type Story = StoryObj<typeof meta>;

// More on writing stories with args: https://storybook.js.org/docs/react/writing-stories/args
export const Example: Story = {
    args: {
        textHeader: 'Welcome to Nodemy',
        title: 'Modal',
        description: 'Đây là phần body của modal',
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const loginButton = await canvas.getByRole('button', {
            name: /close/i,
        });
        await userEvent.click(loginButton);
    },
};