import styled from 'styled-components'

import { breakPoints, effects, transitions } from '@/styles/constants'

interface styleProps {
  asideOpen: boolean
}
export const DropdownContent = styled.ul`
  opacity: 0;
  visibility: hidden;
  float: right;
  position: absolute;
  z-index: 100;
  right: 0;
  top: 4.8rem;

  padding: 1.6rem;

  width: fit-content;
  height: fit-content;
  white-space: nowrap;

  background-color: ${({ theme }) => theme.colors.white};
  box-shadow: ${effects.dropShadow};

  display: flex;
  flex-direction: column;
  align-items: flex-end;

  li {
    font-size: 1.6rem;
    text-align: right;

    cursor: pointer;

    width: 100%;

    & + li {
      margin-top: 0.8rem;
    }

    &:hover {
      color: ${({ theme }) => theme.colors.orangePrimary};
    }
  }
`
export const BodyButton = styled.div<styleProps>`
  border: 0;
  background: transparent;
  position: relative;

  cursor: default;
  > button {
    background: transparent;
    border: 0;
    width: fit-content;
    cursor: pointer;
    transition: all ${transitions.hover};

    display: flex;
    align-items: center;
    font-size: 1.6rem;

    margin-left: 3.2rem;
    padding: 0.8rem;

    &:hover {
      background-color: ${({ theme }) => theme.colors.lightGray};
    }

    &:active {
      font-size: 1.2rem;
    }

    svg {
      margin: 0 1.6rem;
    }
  }
  ${props => props.asideOpen && DropdownContent} {
    opacity: 1;
    visibility: visible;
  }

  ${breakPoints.tablet} {
    margin: 0;
  }
`