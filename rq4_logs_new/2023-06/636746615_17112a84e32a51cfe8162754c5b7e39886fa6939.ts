import { css } from 'lit'

import { getThemeColor, setFont } from '~/utils'

export const msInputStyles = css`
  :host {
    display: block;
  }

  input {
    padding: 8px 16px;
    ${setFont(17, 24)}
    border: 1px solid ${getThemeColor('gray-300')};
    border-radius: 6px;
    width: 100%;
  }

  input:focus {
    outline: none;
    box-shadow: 0px 0px 0px 2px ${getThemeColor('light-primary-color')};
    border-color: ${getThemeColor('admin-user-primary-main')};
  }

  input[disabled] {
    background-color: ${getThemeColor('gray-50-background')};
    cursor: not-allowed;
  }
  input::placeholder {
    color: ${getThemeColor('gray-300')};
  }

  input[type='text'] {
    padding-left: 16px;
  }

  .success {
    border-color: ${getThemeColor('success-main')};
  }

  .success:focus {
    box-shadow: 0px 0px 0px 2px ${getThemeColor('light-success-color')};
    border-color: ${getThemeColor('success-main')};
  }

  .error {
    border-color: ${getThemeColor('error-main')};
  }

  .error:focus {
    box-shadow: 0px 0px 0px 2px ${getThemeColor('light-error-color')};
    border-color: ${getThemeColor('error-main')};
  }

  .message-group {
    display: flex;
    align-items: center;
    gap: 4px;
  }

  .icon {
    width: 16px;
  }

  .message-caption {
    ${setFont(15, 24)}
    color: ${getThemeColor('gray-500')};
  }

  .error-caption {
    ${setFont(15, 24)}
    color: ${getThemeColor('error-main')};
  }

  .success-caption {
    ${setFont(15, 24)}
    color: ${getThemeColor('success-main')};
  }
`