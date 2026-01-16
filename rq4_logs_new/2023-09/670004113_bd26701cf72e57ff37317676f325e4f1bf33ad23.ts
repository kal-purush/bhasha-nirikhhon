import { CustomFlowbiteTheme } from 'flowbite-react';

export const sidebarTheme: CustomFlowbiteTheme = {
	sidebar: {
		root: {
			base: 'h-full',
			inner: 'h-full overflow-y-auto overflow-x-hidden rounded',
		},
		collapse: {
			button:
				'group flex w-full items-center rounded-lg p-2 text-base font-normal text-gray-900 transition duration-75 hover:bg-gray-100 dark:text-white dark:hover:bg-gray-700',
			icon: {
				base: 'h-6 w-6 text-gray-500 transition duration-75 group-hover:text-gray-900 dark:text-gray-400 dark:group-hover:text-white',
				open: {
					off: '',
					on: 'text-gray-900',
				},
			},
			label: {
				base: 'ml-3 flex-1 whitespace-nowrap text-left',
				icon: {
					base: 'h-6 w-6 transition ease-in-out delay-0',
					open: {
						on: 'rotate-180',
						off: '',
					},
				},
			},
			list: 'space-y-2 py-2',
		},
		item: {
			base: 'flex items-center justify-center rounded-lg p-2 text-base font-normal text-gray-900 hover:bg-gray-100 dark:text-white dark:hover:bg-gray-700',
			active: 'bg-gray-100 dark:bg-gray-700',
			collapsed: {
				insideCollapse: 'group w-full pl-8 transition duration-75',
				noIcon: 'font-bold',
			},
			content: {
				base: 'px-3 flex-1 whitespace-nowrap',
			},
			icon: {
				base: 'h-6 w-6 flex-shrink-0 text-gray-500 transition duration-75 group-hover:text-gray-900 dark:text-gray-400 dark:group-hover:text-white',
				active: 'text-gray-700 dark:text-gray-100',
			},
			label: '',
			listItem: '',
		},
		items: '',
		itemGroup:
			'mt-4 space-y-2 border-t border-gray-200 pt-4 first:mt-0 first:border-t-0 first:pt-0 dark:border-gray-700',
	},
};