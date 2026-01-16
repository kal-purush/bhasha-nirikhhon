import { usePrevious } from '@mantine/hooks'
import { type SpringConfig, useSpring } from '@react-spring/web'
import { useEffect } from 'react'

const MODAL_SPRING_CONFIG = {
	damping: 100,
	friction: 100,
	mass: 2.5,
	tension: 1000,
} satisfies SpringConfig

export const useAnimateModalOnOpenChange = ({
	isModalOpen,
}: {
	isModalOpen: boolean
}) => {
	const [overlayPositionerSpring, overlayPositionerSpringApi] = useSpring(
		() => ({
			config: MODAL_SPRING_CONFIG,
			from: { y: '100%' },
		}),
		[],
	)

	const prevIsModalOpen = usePrevious(isModalOpen)

	useEffect(() => {
		if (prevIsModalOpen !== undefined && isModalOpen !== prevIsModalOpen) {
			const closedState = { y: '100%' }
			const openState = { y: '0' }
			overlayPositionerSpringApi.start(() => ({
				config: MODAL_SPRING_CONFIG,
				from: isModalOpen ? closedState : openState,
				to: isModalOpen ? openState : closedState,
			}))
		}
	}, [overlayPositionerSpringApi, isModalOpen, prevIsModalOpen])

	return { overlayPositionerSpring }
}