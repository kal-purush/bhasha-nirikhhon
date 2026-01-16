// Dependencies
import styled from 'styled-components'

// Types
import { themeCssVars, calc } from '../../theme'
import { CalcType } from '../../types'

// Styles
export const DialogWrapper = styled.div`
  background-color: ${themeCssVars.global?.background.paper};
  width: 500px;
  border-radius: ${calc(CalcType.spacing, 4)};
  position: relative;
  z-index: 100;
`

export const DialogBackdrop = styled.div`
  background-color: rgba(0, 0, 0, 0.5);
  position: fixed;
  z-index: 10;
  left: 0;
  right: 0;
  top: 0;
  bottom: 0;
`

export const DialogHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: ${calc(CalcType.spacing, 6)};
  border-bottom: 1px solid ${themeCssVars.global?.background.main};
`

export const DialogHeaderButton = styled.button`
  border: none;
  background-color: transparent;
  padding: 0;
  margin: 0;
  cursor: pointer;
`

export const DialogBody = styled.div`
  padding: ${calc(CalcType.padding, [6, 6, 4])};

  & > * {
    margin-bottom: ${calc(CalcType.spacing, 4)};
  }
`

export const DialogFooter = styled.div`
  display: flex;
  justify-content: flex-end;
  align-items: center;
  padding: ${calc(CalcType.padding, [0, 6, 6, 4])};
`