import styled from "styled-components";
import * as Dialog from '@radix-ui/react-dialog'
import * as RadioGroup from "@radix-ui/react-radio-group";

export const Overlay = styled(Dialog.Overlay)`
  position: fixed;
  width: 100vw;
  height: 100vh;
  inset: 0;
  background: rgba(0, 0, 0, 0.75);
`

export const Content = styled(Dialog.Content)`
  min-width: 32rem;
  border-radius: 6px;
  padding: 2.5rem 3rem;
  background: ${props => props.theme['gray-800']};

  position: fixed;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);

  form {
    margin-top: 2rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;

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

    button[type="submit"] {
      padding: 1rem 2rem;
      border-radius: 6px;
      border: none;
      cursor: pointer;
      margin-top: 1.5rem;

      background-color: ${props => props.theme['green-500']};
      color: ${props => props.theme['white']};
      font-weight: bold;

      &:hover {
        background-color: ${props => props.theme['green-700']};
        transition: 0.2s;
      }
    }
  }
`

export const CloseButton = styled(Dialog.Close)`
  position: absolute;
  background: transparent;
  border: 0;
  top: 1.5rem;
  right: 1.5rem;
  line-height: 0;
  cursor: pointer;
  color: ${props => props.theme['gray-500']};
`

export const TransactionType = styled(RadioGroup.Root)`
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
`

interface TransactionTypeButtonProps {
  variant: 'income' | 'outcome';
}

export const TransactionTypeButton = styled(RadioGroup.Item) <TransactionTypeButtonProps>`
  display: flex;
  gap: 0.5rem;
  justify-content: center;
  align-items: center;

  padding: 1rem 1.5rem;
  border: none;
  border-radius: 6px;
  cursor: pointer;

  color: ${props => props.theme['gray-300']};
  background-color: ${props => props.theme['gray-700']};

  svg {
    color: ${props => props.variant === 'income' ? props.theme['green-300'] : props.theme['red-300']};
  }

  &:hover {
    background-color: ${props => props.theme['gray-600']};
    transition: background-color 0.2s;
  }

  &[data-state="checked"] {
    color: ${props => props.theme['white']};
    background-color: ${props => props.variant === 'income' ? props.theme['green-300'] : props.theme['red-300']};
    

    svg {
      color: ${props => props.theme['white']};
    }
  }
  
`