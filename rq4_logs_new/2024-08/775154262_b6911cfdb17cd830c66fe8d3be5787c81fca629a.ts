import type { Meta, StoryObj } from '@storybook/react';
import { action } from '@storybook/addon-actions';
import theme from '../../../Theme';
import Typography from './Typography';


const meta = {
    component: Typography,
    tags: ['autodocs'],
} as Meta<typeof Typography>;

export default meta;

type Story = StoryObj<typeof Typography>;

const baseProps = {
    variant: "h1",
    children: "Typography",
    component: "h2",
    gutterBottom: true,
    paragraph: true,
    align: 'center',
    color: theme.palette.secondary.light,
    fontSize: "large",
    fontFamily: '"Segoe UI"',
    fontWeight: "bold"

};

export const fullColor: Story = {
    args: {
        ...baseProps,
       

    },
};

export const empty: Story = {
    args: {
        ...baseProps,
        align: 'right',
        color: theme.palette.info.main,
        fontSize: "medium",
        fontFamily: '-apple-system'

    },
};

export const link: Story = {
    args: {
        ...baseProps,
        align: 'left',
        variant: "body1",
        fontSize: "small",
        fontFamily: 'sans-serif',
        color: theme.palette.primary.light,


    },
};
