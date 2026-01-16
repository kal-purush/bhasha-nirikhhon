import { theme } from 'native-base'
import styled from 'styled-components'

export const Overlay = styled.div<{ isOpen: boolean }>`
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: ${theme.colors.black + '75'};
  display: flex;
  justify-content: center;
  align-items: center;
  opacity: ${({ isOpen }) => (isOpen ? '1' : '0')};
  visibility: ${({ isOpen }) => (isOpen ? 'visible' : 'hidden')};
  transition:
    opacity 0.3s ease,
    visibility 0s linear ${({ isOpen }) => (isOpen ? '0s' : '0.3s')};
`

export const Content = styled.section<{ isOpen: boolean }>`
  position: relative;
  background-color: white;
  width: 60%;
  max-width: 880px;
  /* height: 80vh; */
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: flex-start;
  gap: 1rem;
  overflow: auto;

  border-radius: 8px;
  padding: 1.7rem 3rem;
  box-shadow: 0 0 10px ${theme.colors.black + '50'};
  opacity: ${({ isOpen }) => (isOpen ? '1' : '0')};

  & > h1 {
    font-size: 2rem;
    font-weight: 700;
    line-height: 1.5;
    color: ${theme.colors.text['700']};
  }

  @media (max-width: 768px) {
    width: 90%;
    /* height: 90vh; */
    max-width: none;

    padding: 0.5rem 1rem;

    & > h1 {
      font-size: 1.5rem;
      font-weight: 700;
      line-height: 1.5;
    }
  }
`

export const ModalButtonsContainer = styled.div`
  width: 100%;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: flex-end;
`

export const CategoriesContainer = styled.div`
  width: 100%;
`

export const CloseButtonContainer = styled.button`
  position: absolute;
  top: 0;
  right: 0;

  display: flex;
  align-items: center;
  justify-content: center;

  padding: 0.8rem;
  border-radius: 0 8px 0 0;
  transition: 0.3s all ease;

  background-color: ${theme.colors.danger['600']};

  &:hover {
    filter: brightness(70%);
  }

  @media (max-width: 768px) {
    padding: 0.5rem;
  }
`

export const ColorIndicator = styled.div<{ color: string }>`
  width: 4rem;
  height: 2rem;
  border-radius: 5px;
  box-shadow: 0 0 5px rgba(0, 0, 0, 0.3);

  background-color: ${({ color }) => color};
`