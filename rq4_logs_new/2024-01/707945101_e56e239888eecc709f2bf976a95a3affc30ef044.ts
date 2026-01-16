import { useEffect, useRef, useState } from 'react'

export const useRangeInput = <T>(
	range: T[],
	initialValue: T,
	onChange: (values: T) => void,
) => {
	const [value, setValue] = useState<T>(useRef(initialValue).current)

	useEffect(() => onChange(value), [onChange, value])

	const decrement = () =>
		setValue((curr) => {
			const currIndex = range.indexOf(curr)
			const nextIndex = currIndex - 1
			const nextValue = range[nextIndex]

			return nextValue ?? curr
		})

	const increment = () =>
		setValue((curr) => {
			const currIndex = range.indexOf(curr)
			const nextIndex = currIndex + 1
			const nextValue = range[nextIndex]

			return nextValue ?? curr
		})

	return {
		value,
		decrement,
		increment,
	}
}