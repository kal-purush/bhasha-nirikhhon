import { css } from 'lit'

//@TODO: modify styles with var on style-dictionary & theme

export const msToggleStyles = css`
  :host {
    display: inline-block;
  }
  span {
    position: relative;
    display: inline-block;
    width: 32px;
    height: 16px;
    background-color: #ffffff;
    border-radius: 8px;
    border: 1px solid #5e5c64;
    cursor: pointer;
    transition: background-color 0.4s;
  }

  span:hover {
    border-color: #5932c3;
  }

  span:active {
    box-shadow: 0px 0px 0px 2px #ccc0f3;
  }

  span::after {
    content: '';
    position: absolute;
    width: 13px;
    height: 13px;
    border-radius: 50%;
    background-color: #5e5c64;
    top: 1.5px;
    left: 1.5px;
    transition: all 0.4s;
  }

  span:hover::after {
    background-color: #5932c3;
  }

  input:checked + span::after {
    left: 16px;
    background-color: #ffffff;
  }

  input:checked + span {
    background-color: #37363b;
    border-color: #37363b;
  }

  input:checked + span:hover {
    background-color: #5932c3;
    border-color: #5932c3;
  }

  input {
    display: none;
  }

  input[type='checkbox']:disabled + span {
    cursor: not-allowed;
    opacity: 0.5;
  }
`