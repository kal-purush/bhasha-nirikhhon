import { useThrottle } from '@uidotdev/usehooks'
import { useCallback, useEffect, useState } from 'react'

export const useIsPageScrolled = () => {
	const [scroll, setScroll] = useState<'up' | 'down'>('up')

	const throttledScroll = useThrottle(scroll, 250)

	const [lastScroll, setLastScroll] = useState(0)

	const handleScroll = useCallback(() => {
		const currentScroll = window.scrollY

		if (currentScroll > 0 && currentScroll > lastScroll) {
			setScroll('down')
		} else {
			setScroll('up')
		}
		setLastScroll(currentScroll)
	}, [lastScroll])

	useEffect(
		() => window.addEventListener('scroll', handleScroll),
		[handleScroll],
	)

	return throttledScroll === 'up'
}