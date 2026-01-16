export const getSafeAreaInsets = () => {
  const rootStyle = getComputedStyle(document.documentElement)

  return {
    top: parseInt(rootStyle.getPropertyValue('--safe-area-top'), 10) ?? 0,
    right: parseInt(rootStyle.getPropertyValue('--safe-area-right'), 10) ?? 0,
    bottom: parseInt(rootStyle.getPropertyValue('--safe-area-bottom'), 10) ?? 0,
    left: parseInt(rootStyle.getPropertyValue('--safe-area-left'), 10) ?? 0,
  }
}