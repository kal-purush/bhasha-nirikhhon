import { CustomFlowbiteTheme } from 'flowbite-react';

export const ChatHeaderButtonTheme: CustomFlowbiteTheme = {
	button: {
		base: 'group flex items-stretch items-center justify-center text-center font-medium relative focus:z-10 focus:outline-none',
		fullSized: 'w-full',
		color: {
			dark: 'text-white bg-gray-800 border border-transparent enabled:hover:bg-gray-900 focus:ring-4 focus:ring-gray-300 dark:bg-gray-800 dark:enabled:hover:bg-gray-700 dark:focus:ring-gray-800 dark:border-gray-700',
			failure:
				'text-white bg-red-700 border border-transparent enabled:hover:bg-red-800 focus:ring-4 focus:ring-red-300 dark:bg-red-600 dark:enabled:hover:bg-red-700 dark:focus:ring-red-900',
			gray: 'text-gray-900 bg-white border border-gray-200 enabled:hover:bg-gray-100 enabled:hover:text-cyan-700 :ring-cyan-700 focus:text-cyan-700 dark:bg-transparent dark:text-gray-400 dark:border-gray-600 dark:enabled:hover:text-white dark:enabled:hover:bg-gray-700 focus:ring-2',
			info: 'text-white bg-cyan-700 border border-transparent enabled:hover:bg-cyan-800 focus:ring-4 focus:ring-cyan-300 dark:bg-cyan-600 dark:enabled:hover:bg-cyan-700 dark:focus:ring-cyan-800',
			light:
				'text-gray-900 bg-white border border-gray-300 enabled:hover:bg-gray-100 focus:ring-4 focus:ring-cyan-300 dark:bg-gray-600 dark:text-white dark:border-gray-600 dark:enabled:hover:bg-gray-700 dark:enabled:hover:border-gray-700 dark:focus:ring-gray-700',
			purple:
				'text-white bg-purple-700 border border-transparent enabled:hover:bg-purple-800 focus:ring-4 focus:ring-purple-300 dark:bg-purple-600 dark:enabled:hover:bg-purple-700 dark:focus:ring-purple-900',
			success:
				'text-white bg-green-700 border border-transparent enabled:hover:bg-green-800 focus:ring-4 focus:ring-green-300 dark:bg-green-600 dark:enabled:hover:bg-green-700 dark:focus:ring-green-800',
			warning:
				'text-white bg-yellow-400 border border-transparent enabled:hover:bg-yellow-500 focus:ring-4 focus:ring-yellow-300 dark:focus:ring-yellow-900',
			blue: 'text-cyan-900 bg-white border border-cyan-300 enabled:hover:bg-cyan-100 focus:ring-4 focus:ring-cyan-300 :bg-cyan-600 dark:text-white dark:border-cyan-600 dark:enabled:hover:bg-cyan-700 dark:enabled:hover:border-cyan-700 dark:focus:ring-cyan-700',
			cyan: 'text-cyan-900 bg-white border border-cyan-300 enabled:hover:bg-cyan-100 focus:ring-4 focus:ring-cyan-300 :bg-cyan-600 dark:text-white dark:border-cyan-600 dark:enabled:hover:bg-cyan-700 dark:enabled:hover:border-cyan-700 dark:focus:ring-cyan-700',
			green:
				'text-green-900 bg-white border border-green-300 enabled:hover:bg-green-100 focus:ring-4 focus:ring-green-300 :bg-green-600 dark:text-white dark:border-green-600 dark:enabled:hover:bg-green-700 dark:enabled:hover:border-green-700 dark:focus:ring-green-700',
			indigo:
				'text-indigo-900 bg-white border border-indigo-300 enabled:hover:bg-indigo-100 focus:ring-4 focus:ring-indigo-300 :bg-indigo-600 dark:text-white dark:border-indigo-600 dark:enabled:hover:bg-indigo-700 dark:enabled:hover:border-indigo-700 dark:focus:ring-indigo-700',
			lime: 'text-lime-900 bg-white border border-lime-300 enabled:hover:bg-lime-100 focus:ring-4 focus:ring-lime-300 :bg-lime-600 dark:text-white dark:border-lime-600 dark:enabled:hover:bg-lime-700 dark:enabled:hover:border-lime-700 dark:focus:ring-lime-700',
			pink: 'text-pink-900 bg-white border border-pink-300 enabled:hover:bg-pink-100 focus:ring-4 focus:ring-pink-300 :bg-pink-600 dark:text-white dark:border-pink-600 dark:enabled:hover:bg-pink-700 dark:enabled:hover:border-pink-700 dark:focus:ring-pink-700',
			red: 'text-red-900 bg-white border border-red-300 enabled:hover:bg-red-100 focus:ring-4 focus:ring-red-300 :bg-red-600 dark:text-white dark:border-red-600 dark:enabled:hover:bg-red-700 dark:enabled:hover:border-red-700 dark:focus:ring-red-700',
			teal: 'text-teal-900 bg-white border border-teal-300 enabled:hover:bg-teal-100 focus:ring-4 focus:ring-teal-300 :bg-teal-600 dark:text-white dark:border-teal-600 dark:enabled:hover:bg-teal-700 dark:enabled:hover:border-teal-700 dark:focus:ring-teal-700',
			yellow:
				'text-yellow-900 bg-white border border-yellow-300 enabled:hover:bg-yellow-100 focus:ring-4 focus:ring-yellow-300 :bg-yellow-600 dark:text-white dark:border-yellow-600 dark:enabled:hover:bg-yellow-700 dark:enabled:hover:border-yellow-700 dark:focus:ring-yellow-700',
		},
		disabled: 'cursor-not-allowed opacity-50',
		isProcessing: 'cursor-wait',
		spinnerSlot: 'absolute h-full top-0 flex items-center animate-fade-in',
		spinnerLeftPosition: {
			xs: 'left-2',
			sm: 'left-3',
			md: 'left-4',
			lg: 'left-5',
			xl: 'left-6',
		},
		gradient: {
			cyan: 'text-white bg-gradient-to-r from-cyan-400 via-cyan-500 to-cyan-600 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-cyan-300 dark:focus:ring-cyan-800',
			failure:
				'text-white bg-gradient-to-r from-red-400 via-red-500 to-red-600 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-red-300 dark:focus:ring-red-800',
			info: 'text-white bg-gradient-to-r from-cyan-500 via-cyan-600 to-cyan-700 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-cyan-300 dark:focus:ring-cyan-800 ',
			lime: 'text-gray-900 bg-gradient-to-r from-lime-200 via-lime-400 to-lime-500 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-lime-300 dark:focus:ring-lime-800',
			pink: 'text-white bg-gradient-to-r from-pink-400 via-pink-500 to-pink-600 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-pink-300 dark:focus:ring-pink-800',
			purple:
				'text-white bg-gradient-to-r from-purple-500 via-purple-600 to-purple-700 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-purple-300 dark:focus:ring-purple-800',
			success:
				'text-white bg-gradient-to-r from-green-400 via-green-500 to-green-600 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-green-300 dark:focus:ring-green-800',
			teal: 'text-white bg-gradient-to-r from-teal-400 via-teal-500 to-teal-600 enabled:hover:bg-gradient-to-br focus:ring-4 focus:ring-teal-300 dark:focus:ring-teal-800',
		},
		gradientDuoTone: {
			cyanToBlue:
				'text-white bg-gradient-to-r from-cyan-500 to-cyan-500 enabled:hover:bg-gradient-to-bl focus:ring-4 focus:ring-cyan-300 dark:focus:ring-cyan-800',
			greenToBlue:
				'text-white bg-gradient-to-br from-green-400 to-cyan-600 enabled:hover:bg-gradient-to-bl focus:ring-4 focus:ring-green-200 dark:focus:ring-green-800',
			pinkToOrange:
				'text-white bg-gradient-to-br from-pink-500 to-orange-400 enabled:hover:bg-gradient-to-bl focus:ring-4 focus:ring-pink-200 dark:focus:ring-pink-800',
			purpleToBlue:
				'text-white bg-gradient-to-br from-purple-600 to-cyan-500 enabled:hover:bg-gradient-to-bl focus:ring-4 focus:ring-cyan-300 dark:focus:ring-cyan-800',
			purpleToPink:
				'text-white bg-gradient-to-r from-purple-500 to-pink-500 enabled:hover:bg-gradient-to-l focus:ring-4 focus:ring-purple-200 dark:focus:ring-purple-800',
			redToYellow:
				'text-gray-900 bg-gradient-to-r from-red-200 via-red-300 to-yellow-200 enabled:hover:bg-gradient-to-bl focus:ring-4 focus:ring-red-100 dark:focus:ring-red-400',
			tealToLime:
				'text-gray-900 bg-gradient-to-r from-teal-200 to-lime-200 enabled:hover:bg-gradient-to-l enabled:hover:from-teal-200 enabled:hover:to-lime-200 enabled:hover:text-gray-900 focus:ring-4 focus:ring-lime-200 dark:focus:ring-teal-700',
		},
		inner: {
			base: 'flex items-stretch items-center transition-all duration-200',
			position: {
				none: '',
				start: 'rounded-r-none',
				middle: 'rounded-none',
				end: 'rounded-l-none',
			},
			outline: 'border border-transparent',
			isProcessingPadding: {
				xs: 'pl-8',
				sm: 'pl-10',
				md: 'pl-12',
				lg: 'pl-16',
				xl: 'pl-20',
			},
		},
		label:
			'ml-2 inline-flex h-4 w-4 items-center justify-center rounded-full bg-cyan-200 text-xs font-semibold text-cyan-800',
		outline: {
			color: {
				gray: 'border border-gray-900 dark:border-white',
				default: 'border-0',
				light: '',
			},
			off: '',
			on: 'flex justify-center bg-white text-gray-900 transition-all duration-75 ease-in group-enabled:group-hover:bg-opacity-0 group-enabled:group-hover:text-inherit dark:bg-gray-900 dark:text-white w-full',
			pill: {
				off: 'rounded-md',
				on: 'rounded-full',
			},
		},
		pill: {
			off: 'rounded-lg',
			on: 'rounded-full',
		},
		size: {
			xs: 'text-xs px-2 py-1',
			sm: 'text-sm px-1.5 py-1.5',
			md: 'text-sm px-4 py-2',
			lg: 'text-base px-5 py-2.5',
			xl: 'text-base px-6 py-3',
		},
	},

	avatar: {
		root: {
			base: 'flex justify-center items-center space-x-4 rounded',
			bordered: 'p-1 ring-2',
			rounded: 'rounded-full',
			color: {
				dark: 'ring-gray-800 dark:ring-gray-800',
				failure: 'ring-red-500 dark:ring-red-700',
				gray: 'ring-gray-500 dark:ring-gray-400',
				info: 'ring-cyan-400 dark:ring-cyan-800',
				light: 'ring-gray-300 dark:ring-gray-500',
				purple: 'ring-purple-500 dark:ring-purple-600',
				success: 'ring-green-500 dark:ring-green-500',
				warning: 'ring-yellow-300 dark:ring-yellow-500',
				pink: 'ring-pink-500 dark:ring-pink-500',
			},
			img: {
				base: 'rounded',
				off: 'relative overflow-hidden bg-gray-100 dark:bg-gray-600',
				on: '',
				placeholder: 'absolute w-auto h-auto text-gray-400 -bottom-1',
			},
			size: {
				xs: 'w-6 h-6',
				sm: 'w-8 h-8',
				md: 'w-10 h-10',
				lg: 'w-20 h-20',
				xl: 'w-36 h-36',
			},
			stacked: 'ring-2 ring-white dark:ring-gray-500',
			statusPosition: {
				'bottom-left': '-bottom-1 -left-1',
				'bottom-center': '-bottom-1 center',
				'bottom-right': '-bottom-1 -right-1',
				'top-left': '-top-1 -left-1',
				'top-center': '-top-1 center',
				'top-right': '-top-1 -right-1',
				'center-right': 'center -right-1',
				center: 'center center',
				'center-left': 'center -left-1',
			},
			status: {
				away: 'bg-yellow-400',
				base: 'absolute h-3.5 w-3.5 rounded-full border-2 border-white dark:border-gray-800',
				busy: 'bg-red-400',
				offline: 'bg-gray-400',
				online: 'bg-green-400',
			},
			initials: {
				text: 'font-medium text-gray-600 dark:text-gray-300',
				base: 'inline-flex overflow-hidden relative justify-center items-center bg-gray-100 dark:bg-gray-600',
			},
		},
		group: {
			base: 'flex -space-x-2.5',
		},
		groupCounter: {
			base: 'relative flex items-center justify-center w-10 h-10 text-xs font-medium text-white bg-gray-700 rounded-full ring ring-white hover:bg-gray-600 dark:ring-gray-500',
		},
	},
};