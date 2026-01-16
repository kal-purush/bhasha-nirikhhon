import { styled } from "styled-components";

export const ModalHeaderContainer = styled.ul`
  display: flex;
  flex-direction: column;
  gap: .7rem;

  position: absolute;
  top: 64.5px;
  right: 28px;

  padding: 1rem;

  background: var(--white);

  border-radius: 0 0 6px 6px ;

  box-shadow: rgba(0, 0, 0, 0.1) 0px 4px 12px;

  transform: translateY(100%);
  opacity: 0;

  transition: transform 0.3s ease, opacity 0.3s ease;
`

export const ModalItem = styled.li`
  color: var(--gray);

  cursor: pointer;
  font-size: .8rem;
  
  &:hover {
    color: var(--black);
  }
`