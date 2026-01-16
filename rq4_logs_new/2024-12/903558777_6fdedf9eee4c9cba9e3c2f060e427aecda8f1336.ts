import styled from "styled-components";

export const SearchATransactionContainer = styled.div`
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;

  input {
    padding: 1rem;
    border-radius: 6px;
    background-color: ${props => props.theme['gray-900']};
    border: none;
    width: 100%;
    color: ${props => props.theme['gray-300']};
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
  background-color: transparent;
  font-weight: bold;
  line-height: 1.6;

  cursor: pointer;

  display: flex;
  gap: 0.75rem;

  &:hover {
    background: ${props => props.theme['green-500']};
    color: ${props => props.theme['white']};
    border-color: ${props => props.theme['green-500']};
    transition: background-color 0.2s, color 0.2s, border-color 0.2s;
  }
}
`