import type { Meta, StoryObj } from '@storybook/react';

import { ProductCard } from './ProductCard';

const meta = {
    title: 'Card/ProductCard',
    component: ProductCard,
    parameters: {
        layout: 'centered',
    },
    tags: ['autodocs'],
    argTypes: {
        image: { control: 'text' },
        name: { control: 'text' },
        price: { control: 'number' },
    },
} satisfies Meta<typeof ProductCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: {
        image: 'images/Nike Zoom KD 12.png',
        name: 'Nike Zoom KD 12',
        price: 98,
    },
};