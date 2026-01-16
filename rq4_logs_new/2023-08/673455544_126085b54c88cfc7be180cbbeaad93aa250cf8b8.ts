import { styled } from "styled-components";

export const ModalCardContainer = styled.ul`
  display: flex;
  flex-direction: column;
  gap: .7rem;

  position: absolute;
  top: 194.5px;
  right: 10px;

  padding: 1rem;

  background: var(--white);

  border-radius: 6px;

  box-shadow: rgba(50, 50, 93, 0.25) 0px 6px 12px -2px, rgba(0, 0, 0, 0.3) 0px 3px 7px -3px;

  transform: translateY(100%);
  opacity: 0;

  transition: transform 0.3s ease, opacity 0.3s ease;
`

export const CardModalItem = styled.li`
  color: var(--gray);

  cursor: pointer;
  font-size: .8rem;
  
  &:hover {
    color: var(--black);
  }
`

export const CloseThisModal = styled.button`
  cursor: pointer;

  position: absolute;
  top: 4px;
  right: 8px;

  color: var(--gray);

  &:hover {
    color: var(--black);
  }
`