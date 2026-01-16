import type { Meta, StoryObj } from '@storybook/react';
import { action } from '@storybook/addon-actions';
import theme from '../../../Theme';
import Typography from './Typography';


const meta = {
    component: Typography ,
    tags: ['autodocs'],
} as Meta<typeof Typography>;

export default meta;

type Story = StoryObj<typeof Typography>;

const baseProps = {
    children: "Typography",
    gutterBottom: true,
    fontWeight: "bold"   

};

export const Header: Story = {
    args: {
        ...baseProps,
        variant: "h2",
        color: "primary",
        align: "center",
        component: "div" as React.ElementType<any>,
       

    },
};
export const CommonText: Story = {
    args: {
        ...baseProps,
        variant: "body1",
        color: "secondary",
        align: "left",
        component: "div" as React.ElementType<any>,


    },
};

export const subTitle: Story = {
    args: {
        ...baseProps,
        variant: "subtitle1",
        color: "success",
        align: "right",
        component: "div" as React.ElementType<any>,
       
      
      


    },
};
