import { Button } from "./button";
import type { Meta, StoryObj } from "@storybook/react";

const meta = {
  title: "Components/Button",
  component: Button,
  tags: ["autodocs"],
} satisfies Meta<typeof Button>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Primary: Story = {
  args: {
    size: "medium",
    children: "Primary",
    type: "primary",
  },
};

export const Secondary: Story = {
  args: {
    children: "Secondary",
    type: "secondary",
  },
};

export const Transparent: Story = {
  args: {
    children: "Transparent",
    type: "transparent",
  },
};

export const Link: Story = {
  args: {
    children: "Link",
    type: "link",
  },
};