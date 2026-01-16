'use client'

import { useHotkeys } from '@mantine/hooks'
import { useAtomValue, useSetAtom } from 'jotai'
import { useRouter } from 'next/navigation'

import {
	type FONT_SIZE_OFFSET,
	fontSizeOffsetAtom,
	fontSizeOffsetDefaultValue,
	isFirstChapterAtom,
	isLastChapterAtom,
	nextChapterURLAtom,
	prevChapterURLAtom,
} from '~/state'

export const UseReaderHotKeys = () => {
	const setFontSizeOffset = useSetAtom(fontSizeOffsetAtom)

	const decreaseFontSize = () =>
		setFontSizeOffset((prev) => {
			const newValue = prev - 1
			return newValue >= -2 ? (newValue as FONT_SIZE_OFFSET) : prev
		})

	const increaseFontSize = () =>
		setFontSizeOffset((prev) => {
			const newValue = prev + 1
			return newValue <= 8 ? (newValue as FONT_SIZE_OFFSET) : prev
		})

	const resetFontSize = () => setFontSizeOffset(fontSizeOffsetDefaultValue)

	const router = useRouter()

	const prevChapterURL = useAtomValue(prevChapterURLAtom)
	const isFirstChapter = useAtomValue(isFirstChapterAtom)
	const goToPrevChapter = () => !isFirstChapter && router.push(prevChapterURL)

	const nextChapterURL = useAtomValue(nextChapterURLAtom)
	const isLastChapter = useAtomValue(isLastChapterAtom)
	const goToNextChapter = () => !isLastChapter && router.push(nextChapterURL)

	useHotkeys([
		['mod+-', decreaseFontSize],
		['mod+=', increaseFontSize],
		['mod+0', resetFontSize],
		['ArrowLeft', goToPrevChapter],
		['ArrowRight', goToNextChapter],
	])

	return null
}