import { createSlice } from '@reduxjs/toolkit'
import type { RootState } from '../store'

interface SidebarState {
    isOpen: boolean
}

const initialState: SidebarState = {
    isOpen: false,
}

export const sidebarSlice = createSlice({
    name: 'sidebar',
    initialState,
    reducers: {
        toggleSidebar: state => {
            state.isOpen = !state.isOpen
        },
        setSidebarOpen: (state, action) => {
            state.isOpen = action.payload
        },
    },
})

export const { toggleSidebar, setSidebarOpen } = sidebarSlice.actions

// Selector
export const selectSidebarOpen = (state: RootState) => state.sidebar.isOpen

export default sidebarSlice.reducer