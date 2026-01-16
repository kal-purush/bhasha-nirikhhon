import { Meta, StoryObj } from '@storybook/react'

import { Header } from '@/components/shared/header'

export default {
  title: 'Shared/Header/Header',
  component: Header,
  parameters: {
    layout: 'fullscreen',
  },
} as Meta

export const Default: StoryObj = {
  args: {
    isAuthenticated: false,
    user: null,
    currentPage: '/project',
  },
}
export const WithUserMenu: StoryObj = {
  args: {
    isAuthenticated: true,
    user: {
      id: '2',
      name: 'Alexander',
      imageUrl: 'https://picsum.photos/250/250',
    },
    currentPage: '/portfolio',
  },
}
export const LoggedInWithoutUser: StoryObj = {
  args: {
    isAuthenticated: true,
    user: null,
    currentPage: '/team',
  },
}