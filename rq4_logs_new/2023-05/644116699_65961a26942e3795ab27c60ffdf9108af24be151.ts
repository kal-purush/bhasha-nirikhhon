import { create } from 'zustand'
import { getStoredSettings } from '../storage/settings'

interface SettingsStore {
	focusLength: number
	shortBreakLength: number
	longBreakLength: number
	hasNotifications: boolean
	setFocusLength: (length: number) => void
	setShortBreakLength: (length: number) => void
	setLongBreakLength: (length: number) => void
	setHasNotifications: (enabled: boolean) => void
}

const initialSettings = await getStoredSettings()

export const useSettings = create<SettingsStore>((set) => ({
	focusLength: initialSettings?.focusLength ?? 30,
	shortBreakLength: initialSettings?.shortBreakLength ?? 5,
	longBreakLength: initialSettings?.longBreakLength ?? 15,
	hasNotifications: !!initialSettings?.notifications,
	setFocusLength: (length) => set({ focusLength: length }),
	setLongBreakLength: (length) => set({ longBreakLength: length }),
	setShortBreakLength: (length) => set({ shortBreakLength: length }),
	setHasNotifications: (enabled) => set({ hasNotifications: enabled }),
}))