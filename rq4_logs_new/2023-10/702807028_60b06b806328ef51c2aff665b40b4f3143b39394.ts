import type { Meta, StoryObj } from '@storybook/react';
import { ProfilesCard } from './ProfilesCard';

const meta = {
    title: 'Card/ProfileCard',
    component: ProfilesCard,
    parameters: {
        layout: 'centered',
    },
    tags: ['autodocs'],
    argTypes: {
        userList: { control: 'object' },
    },
} satisfies Meta<typeof ProfilesCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
    args: {
        userList: [
            {
                id: 1,
                name: 'Dinh Nam',
                role: 'Fullstack Developer',
                avatar: 'images/aNam.jpg',
            },
            {
                id: 2,
                name: 'Minh Thai',
                role: 'Fullstack Developer',
                avatar: 'images/aNam.jpg',
            },
            {
                id: 3,
                name: 'Cuonng nv',
                role: 'Fullstack Developer',
                avatar: 'images/aNam.jpg',
            },
        ],
    },
};