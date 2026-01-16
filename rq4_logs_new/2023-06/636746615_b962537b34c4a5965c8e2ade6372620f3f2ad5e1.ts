import type { Meta, StoryObj } from '@storybook/web-components'
import { html } from 'lit'

import './ms-typography'

export default {
  title: 'Typography',
  argTypes: {
    align: {
      options: ['inherit', 'center', 'left', 'right', 'justify'],
      control: { type: 'select' },
    },
    variant: {
      options: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'span', 'article'],
      control: { type: 'select' },
    },
    weight: {
      options: ['300', '400', '700'],
      control: { type: 'select' },
    },
  },
  render: (args) =>
    html` <ms-typography
      id="test"
      .align=${args.align}
      .variant=${args.variant}
      .color=${args.color}
      .weight=${args.weight}
    >
      ${args.slot}
    </ms-typography>`,
} as Meta

export const Checkbox: StoryObj = {
  name: 'MSTypography',
  args: {
    align: 'inherit',
    variant: 'h1',
    slot: 'Here a text as h1',
    color: 'red',
    weight: '400',
  },
}