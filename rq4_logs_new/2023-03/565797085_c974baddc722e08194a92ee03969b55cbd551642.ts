import { createSlice, PayloadAction } from '@reduxjs/toolkit'
import { Customization, customizationConfig, Mode } from 'application/theme'
import { getLocalStorage } from 'shared/utils'

const initialState: Customization = {
  fontFamily: getLocalStorage<Customization>('customization')?.fontFamily || customizationConfig.fontFamily,
  fontSize: getLocalStorage<Customization>('customization')?.fontSize || customizationConfig.fontSize,
  borderRadius: getLocalStorage<Customization>('customization')?.borderRadius || customizationConfig.borderRadius,
  mode: getLocalStorage<Customization>('customization')?.mode || customizationConfig.mode, // todo: додати системну тему
  compactAlgorithm:
    getLocalStorage<Customization>('customization')?.compactAlgorithm || customizationConfig.compactAlgorithm,
  colorPrimary: getLocalStorage<Customization>('customization')?.colorPrimary || customizationConfig.colorPrimary,
  drawerPlacement:
    getLocalStorage<Customization>('customization')?.drawerPlacement || customizationConfig.drawerPlacement
}

export const customizationSlice = createSlice({
  name: 'customization',
  initialState,
  reducers: {
    setFontFamily: (state, action: PayloadAction<string>) => {
      state.fontFamily = action.payload
    },
    setBorderRadius: (state, action: PayloadAction<number>) => {
      state.borderRadius = action.payload
    },
    setMode: (state, action: PayloadAction<Mode>) => {
      state.mode = action.payload
    },
    setCompactAlgorithm: (state, action: PayloadAction<boolean>) => {
      state.compactAlgorithm = action.payload
    },
    setFontSize: (state, action: PayloadAction<number>) => {
      state.fontSize = action.payload
    },
    setPrimaryColor: (state, action: PayloadAction<string>) => {
      state.colorPrimary = action.payload
    }
  }
})

export const { setFontFamily, setBorderRadius, setMode, setPrimaryColor, setCompactAlgorithm } =
  customizationSlice.actions
export const customizationReducer = customizationSlice.reducer