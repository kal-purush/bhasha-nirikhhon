import styled from 'styled-components'
import colors from '../../styles/colors'
import { standardShadow } from '../../styles/globals'

export const Container = styled.div<{ show: boolean }>`
  background: ${colors.lightBg};
  position: fixed;

  z-index: 50;
  top: 0;
  bottom: 0;
  height: 100%;
  right: 0;
  width: 90%;

  transform: ${(props) => (props.show ? 'translateX(0)' : 'translateX(100%)')};
  transition: transform 0.3s;

  ${standardShadow};
`
export const PanelHeader = styled.div`
  padding: 1rem 1.5rem;
`

export const ClosePanelButton = styled.button`
  background: transparent;
  color: lightgray;
  border: 0;
  font-size: 17px;
  padding: 0.5rem 0;

  svg {
    vertical-align: middle;
  }

  :active {
    color: ${colors.extraLightGray};
  }
`

export const PanelSubHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 0;
`

export const PanelTitle = styled.h3`
  color: ${colors.primary};
  display: flex;
  align-items: center;
  font-size: 22px;
  font-weight: 500;

  svg {
    margin-right: 0.5rem;
  }
`

export const PanelSubtitle = styled.p`
  color: ${colors.lightGray};
  font-size: 18px;
  font-weight: 400;
`

export const PanelBody = styled.div`
  padding: 1rem;
`

export const PanelFooter = styled.div`
  padding: 1rem;
`