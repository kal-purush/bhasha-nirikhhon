import { ClassNamesConfig } from 'react-select';

import { cn } from '@/lib/utils';

export const classNames = (extraControlClassName?: string): ClassNamesConfig => ({
	control: ({ isFocused, isDisabled }) =>
		cn(
			'bg-gradient min-h-10 rounded-md border border-input px-3 py-2 text-sm text-foreground',
			isFocused && 'outline outline-2 outline-offset-2 outline-secondary',
			isDisabled &&
				'cursor-not-allowed border-destructive/50 !from-destructive/5 !to-destructive/5 text-destructive',
			extraControlClassName
		),
	placeholder: () => 'text-muted-foreground text-sm',
	input: () => 'grow',
	singleValue: () => 'grow',
	multiValue: () => 'border border-input px-2 rounded bg-base-200',
	valueContainer: () => 'flex flex-wrap gap-2',
	menu: () => 'bg-base overflow-hidden  rounded shadow-2xl text-sm border border-input p-1',
	option: ({ isFocused, isSelected }) =>
		cn(
			'rounded px-3 py-1.5 text-sm text-foreground',
			isFocused && 'bg-base-200',
			isSelected && 'before:mr-1 before:content-["✔"]'
		),
	noOptionsMessage: () => 'text-destructive text-sm p-2 bg-destructive/5 border border-destructive/20 rounded',
});