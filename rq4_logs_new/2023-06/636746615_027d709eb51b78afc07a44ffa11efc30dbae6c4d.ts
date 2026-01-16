import type { Meta, StoryObj } from '@storybook/web-components'
import { html } from 'lit'

import { positionsAllowed, typesAllowed } from './alert'

export default {
  title: 'Alert',
  argTypes: {
    variant: {
      options: typesAllowed,
      control: { type: 'select' },
    },
    position: {
      options: positionsAllowed,
      control: { type: 'select' },
    },
    renderAsToast: {
      control: { type: 'boolean' },
    },
    hasCloseButton: {
      control: { type: 'boolean' },
    },
  },
  render: (args) =>
    html`
      <ms-alert
        .title=${args.title}
        .renderAsToast=${args.renderAsToast}
        .hasCloseButton="${args.hasCloseButton}"
        .variant=${args.variant}
        .description=${args.description}
        .position=${args.position}
      >
      </ms-alert>
    `,
} as Meta

export const Checkbox: StoryObj = {
  name: 'MSAlert',
  args: {
    title: 'Alert',
    variant: 'error',
    renderAsToast: true,
    hasCloseButton: false,
    description: 'description',
    position: 'top-left',
  },
}