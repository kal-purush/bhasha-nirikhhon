import css from '@emotion/css/macro';
import { BorderInfoState } from '../../styles/border';
import { theme } from '../../styles/theme';
import { centerFlex, pointer } from '../../styles/common';

const getColourFromInfoState = (state: BorderInfoState) => {
  switch (state) {
    case BorderInfoState.ERROR:
      return theme.colours.errorRed;
    case BorderInfoState.WARN:
      return theme.colours.warningAmber;
    default:
      return theme.colours.infoGreen;
  }
};

export const styledFeedbackInput = (
  state: BorderInfoState = BorderInfoState.INFO,
  hidden: boolean,
  message?: string
) => {
  const borderColour = getColourFromInfoState(state);
  const iconStyle = css`
    .feedback-icon {
      width: 13px;
      height: 13px;
      position: absolute;
      right: 5px;
      font-size: ${theme.font.size.small};
      font-weight: ${theme.font.weight.bold};
      color: ${borderColour};
      border-radius: ${theme.border.radius.full};
      border: 2px solid ${borderColour};
      ${centerFlex};
      ${pointer};
    }
  `;

  const inputStyle = css`
    input {
      border: none;
      padding-right: 20px;
    }
  `;

  const messageStyle = css`
    .feedback-icon:after {
      content: '${message}';
      display: none;
      position: absolute;
      white-space: nowrap;
      border: ${theme.border.thin(borderColour)};
      border-radius: ${theme.border.radius.medium};
      padding: 2px;
      bottom: 150%;
      background: white;
      opacity: 0.9;
      font-size: ${theme.font.size.xSmall};
    }
    .feedback-icon:hover:after {
      display: block;
    }
  `;

  return css`
    border: ${theme.border.thin(theme.colours.background.hoverGrey)};
    ${
      !hidden &&
      css`
        border: 2px solid ${borderColour};
      `
    };
    border-radius: ${theme.border.radius.medium};
    display: flex;
    position: relative;
    align-items: center;
    ${iconStyle}
    ${inputStyle}
    ${messageStyle}
  `;
};