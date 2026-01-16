import styled from 'styled-components';

export const StyledApp = styled.div`
  .load-more-button {
    width: 100%;
    height: 35px;
    border-radius: 5px;
    border: none;
    background-color: var(--color-light);
  }

  .load-more-button:is(:hover, :focus) {
    background-color: var(--color-primary);
    outline: 2px solid var(--color-dark);
  }
`;