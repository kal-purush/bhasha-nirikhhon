import { useThemeUI } from 'theme-ui'

export default function useImageBackgroundColor() {
  const { colorMode, theme } = useThemeUI()
  if (colorMode === 'light') {
    return theme.colors.muted
  } else {
    return theme.colors.modes[colorMode].muted
  }
}