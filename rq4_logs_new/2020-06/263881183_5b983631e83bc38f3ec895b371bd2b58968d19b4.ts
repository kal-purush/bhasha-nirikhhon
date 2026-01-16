import css from '@emotion/css/macro';

import { FeedbackStatus } from '../../ducks/toast/ToastTypes';
import { getColourFromStatus } from '../../styles/colours';
import { pointer, unselectable } from '../../styles/common';
import { theme } from '../../styles/theme';
import { fadeIn, fadeOut } from '../../styles/keyframes';

export const styledToast = (state: FeedbackStatus) => {
  const borderColour = getColourFromStatus(state);
  const backgroundColour = getColourFromStatus(state, true);
  const headerStyle = css`
    .header {
      display: flex;
      justify-content: space-between;
      line-height: 1;
      font-size: ${theme.font.size.small};
      font-weight: ${theme.font.weight.bold};
      color: ${borderColour};
      padding: ${theme.spacing.small};
      padding-bottom: 0;
      ${unselectable}
      .close {
        ${pointer}
      }
    }
  `;

  const messageStyle = css`
    .message {
      padding: ${theme.spacing.medium};
      padding-top: 0;
    }
  `;

  const show = css`
    &.show {
      display: block;
      animation: ${fadeIn} 0.5s;
    }
  `;

  const hide = css`
    &.hidden {
      opacity: 0;
      animation: ${fadeOut} 0.5s;
    }
  `;

  return css`
    display: none;
    position: fixed;
    left: ${theme.spacing.medium};
    right: ${theme.spacing.medium};
    border: ${theme.border.medium(borderColour)};
    border-radius: ${theme.border.radius.medium};
    bottom: ${theme.spacing.medium};
    background-color: ${backgroundColour};
    ${headerStyle}
    ${messageStyle}
    ${show}
    ${hide}
  `;
};