import { uiFonts, uiTheme } from '@tabula/ui-theme';

import { buildVariants } from './variants.css';

const rootStyle = {
  gap: '4px',
  height: '20px',
  padding: '0 12px',
  borderRadius: '6px',
  outlineWidth: '2px',

  icon: {
    paddingLeft: '8px',
  },
};

const variantStyles = {
  contract: {
    font: uiFonts.sansSerif.semiBold12,
    default: {
      borderColor: uiTheme.colors.whiteAlpha['30'],
      color: uiTheme.colors.content.contrast,
    },
    hover: {
      background: uiTheme.colors.whiteAlpha['10'],
      borderColor: uiTheme.colors.whiteAlpha['50'],
    },
    active: {
      borderColor: uiTheme.colors.whiteAlpha['70'],
    },
    focus: {
      outlineColor: uiTheme.colors.whiteAlpha['30'],
    },
    disabled: {
      background: uiTheme.colors.whiteAlpha['10'],
      borderColor: 'transparent',
      color: uiTheme.colors.content.contrastDisabled,
    },
  },
};

export const variants = buildVariants(rootStyle, variantStyles);