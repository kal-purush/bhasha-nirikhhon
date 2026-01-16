export const rgbToHex = (pixel: Uint8ClampedArray): string => {
  return `#${((1 << 24) + (pixel[0] << 16) + (pixel[1] << 8) + pixel[2])
    .toString(16)
    .slice(1)}`
}

interface Dimensions {
  width: number
  height: number
}

export const calculateAspectRatioFit = (
  imageWidth: number,
  imageHeight: number,
  containerWidth: number,
  containerHeight: number,
  padding: number = 0
): Dimensions => {
  const aspectRatio = imageWidth / imageHeight

  let newWidth = containerWidth
  let newHeight = newWidth / aspectRatio

  if (newHeight > containerHeight) {
    newHeight = containerHeight
    newWidth = newHeight * aspectRatio
  }

  if (newWidth > containerWidth) {
    newWidth = containerWidth
    newHeight = newWidth / aspectRatio
  }

  return {
    width: newWidth - padding,
    height: newHeight - padding,
  }
}