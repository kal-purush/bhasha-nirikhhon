import type { Meta, StoryObj } from '@storybook/react'

import { SelectBox } from './SelectBox'

const meta = {
  component: SelectBox,
  tags: ['autodocs'],
  title: 'Components/SelectBox',
} satisfies Meta<typeof SelectBox>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    placeholder: 'Value',
  },
}

export const Label: Story = {
  args: {
    placeholder: 'Value',
    label: 'Some label',
  },
}

export const SetWidth: Story = {
  args: {
    placeholder: 'Value',
    label: 'Some label',
    width: 210,
  },
}

export const IsDisabled: Story = {
  args: {
    placeholder: 'Value',
    label: 'Some label',
    disabled: true,
  },
}