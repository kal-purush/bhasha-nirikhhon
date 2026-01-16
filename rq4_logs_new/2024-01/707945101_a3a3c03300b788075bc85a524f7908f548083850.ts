import { useEffect } from 'react'

export const UseDisableVercelLiveFeedback = () => {
	useEffect(() => document.querySelector('vercel-live-feedback')?.remove(), [])

	return null
}