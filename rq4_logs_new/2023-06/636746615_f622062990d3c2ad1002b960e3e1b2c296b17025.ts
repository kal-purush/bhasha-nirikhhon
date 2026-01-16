import { css } from 'lit'

import { getThemeColor } from '~/utils'

export const msTagStyles = css`
  :host {
    display: inline-flex;
    width: fit-content;
    height: 24px;
    box-sizing: border-box;
    padding: 4px 8px 4px 8px;
    justify-content: center;
    align-items: center;
    gap: 4px;
    border-radius: 24px;
    background-color: ${getThemeColor('admin-user-primary-background')};
  }
  svg {
    fill: ${getThemeColor('admin-user-primary-dark')};
    cursor: pointer;
    margin-left: 4px;
  }
`