import { css } from 'lit';

export const myElementStyles = css`
  /* :host styles */
  :host {
    display: block;
    color: var(--my-element-color);
    padding: 2rem;
    max-width: 800px;
    font-family: var(--my-element-font-family);
  }

  /* button styles */
  button,
  input {
    color: var(--my-element-button-color);
    background-color: var(--my-element-background-color);
    border-radius: 4px;
    padding: 0.5rem 1rem;
    font-size: 1rem;
  }

  input {
    background-color: var(--my-element-color);
  }
`;