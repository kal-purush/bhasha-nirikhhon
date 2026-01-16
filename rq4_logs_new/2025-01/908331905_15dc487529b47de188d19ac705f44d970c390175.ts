import type { Meta, StoryObj } from '@storybook/react'

import { MenuItemsType, menuItems } from '@/src/components/navigationPanel/NavigationPanel'
import Sidebar from '@/src/components/navigationPanel/sidebar/Sidebar'

const meta = {
  component: Sidebar,
  tags: ['autodocs'],
  title: 'Components/Sidebar',
} satisfies Meta<typeof Sidebar>

export default meta
type Story = StoryObj<typeof meta>

const menuItemsMock: MenuItemsType = menuItems

export const Primary: Story = {
  args: {
    items: {
      additional: menuItemsMock.additional,
      mainActions: menuItemsMock.mainActions,
      usersActions: menuItems.usersActions,
    },
  },
}