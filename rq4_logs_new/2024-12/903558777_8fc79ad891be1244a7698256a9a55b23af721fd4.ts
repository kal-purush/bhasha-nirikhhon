import styled from "styled-components";

export const TransactionsTableContainer = styled.main`
  display: flex;
  flex-direction: column;
  gap: 1.5rem;

  margin: 4rem auto 0;
  width: 100%;
  max-width: 1120px;
  padding: 0 1.5rem;
`

export const SearchATransactionContainer = styled.div`
  display: flex;
  gap: 1rem;

  input {
    padding: 1rem;
    border-radius: 6px;
    background-color: ${props => props.theme['gray-900']};
    border: none;
    width: 100%;
    color: ${props => props.theme['gray-300']};
    font: 400 1rem Roboto, sans-serif;
    line-height: 1.4;

    ::placeholder {
      color: ${props => props.theme['gray-500']};
    }
  }

  button {
    padding: 0.875rem 2rem;
    border: 1px solid ${props => props.theme['green-300']};
    color: ${props => props.theme['green-300']};
    border-radius: 6px;
    background-color: ${props => props.theme['gray-800']};
    font-weight: bold;
    line-height: 1.6;

    cursor: pointer;

    display: flex;
    gap: 0.75rem;

    &:hover {
      background-color: ${props => props.theme['green-300']};
      color: #fff;
      transition: 0.2s;
    }
  }
`

export const TransactionsTable = styled.table`
  width: 100%;
  border-collapse: separate;
  border-spacing: 0 0.5rem;



  td {
    padding: 1.25rem 2rem;
    background: ${props => props.theme['gray-700']};

    &:first-child {
      border-top-left-radius: 6px;
      border-bottom-left-radius: 6px;
      width: 40%;
    }

    &:last-child {
      border-top-right-radius: 6px;
      border-bottom-right-radius: 6px;
    }
  }
`

interface PriceHighlight {
  variant?: 'income';
}

export const PriceHighlight = styled.span<PriceHighlight>`
  color: ${props => props.variant ? props.theme['green-300'] : props.theme['red-300']}
`