import type { Meta, StoryObj } from '@storybook/react';

import { Gallery } from './Gallery';

const meta = {
    title: 'Gallery',
    component: Gallery,
    parameters: {
        layout: 'centered',
    },

    tags: ['autodocs'],
    argTypes: {
        images: { control: 'object' }, // mean that images is an array
    },
} satisfies Meta<typeof Gallery>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Example: Story = {
    args: {
        images: [
            { id: 1, src: 'images/img1.jpeg' },
            { id: 2, src: 'images/img2.jpeg' },
            { id: 3, src: 'images/img3.jpeg' },
            { id: 4, src: 'images/img4.jpeg' },
            { id: 5, src: 'images/img5.jpeg' },
            { id: 6, src: 'images/img6.jpeg' },
            { id: 7, src: 'images/img7.jpeg' },
            { id: 8, src: 'images/img8.jpeg' },
        ],
    },
};