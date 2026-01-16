import { merge } from 'lodash'
import { PaletteProps, defaultPalette } from './createPalette'

const lineWidth = '1px'
const lineStyle = 'solid'
const lineColor = defaultPalette.BORDER

export type FrameProps = {
  border?: {
    lineWitdh?: string
    lineStyle?: string
    default?: string
    radius?: {
      s?: string
      m?: string
      l?: string
    }
  }
}

export type CreatedFrameTheme = {
  border?: {
    lineWidth?: string
    lineStyle?: string
    default?: string
    radius?: {
      s?: string
      m?: string
    }
  }
}

export const defaultFrame: CreatedFrameTheme = {
  border: {
    lineWidth,
    lineStyle,
    default: `${lineWidth} $l{lineStyle} ${lineColor}`,
    radius: {
      s: '4px',
      m: '6px',
    },
  },
}

export const createFrame = (
  userFrame: FrameProps = {},
  userPalette: PaletteProps = {}
): CreatedFrameTheme => {
  const color = userPalette.BORDER || defaultPalette.BORDER
  const created: CreatedFrameTheme = merge({
    border: {
      ...defaultFrame.border,
      default: `${lineWidth} ${lineStyle} ${color}`,
    },
    userFrame,
  })

  return created
}