import { useMemo } from 'preact/hooks'
import Color from 'color'

const getLuminance = (colour: Color) => {
  const a = [colour.red(), colour.green(), colour.blue()].map((v) => {
    v /= 255
    return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4)
  })

  return a[0] * 0.2126 + a[1] * 0.7152 + a[2] * 0.0722
}

const getContrastRatio = (a: Color, b: Color) => {
  const lumaA = getLuminance(a)
  const lumaB = getLuminance(b)

  return lumaA > lumaB
    ? (lumaB + 0.05) / (lumaA + 0.05)
    : (lumaA + 0.05) / (lumaB + 0.05)
}

const useUserColours = (colour?: string, defaultColour = '#ff4b97') =>
  useMemo(() => {
    let userColour = Color(colour || defaultColour).hsl()

    if (userColour.saturationl() > 85) {
      userColour = userColour.saturationl(85)
    }

    if (userColour.saturationl() < 45) {
      userColour = userColour.saturationl(45)
    }

    let highlightColour = new Color(userColour).lightness(95)

    if (getContrastRatio(highlightColour, userColour) > 0.65) {
      highlightColour = highlightColour.lightness(12)
    }

    return [userColour, highlightColour]
  }, [colour, defaultColour])

export default useUserColours