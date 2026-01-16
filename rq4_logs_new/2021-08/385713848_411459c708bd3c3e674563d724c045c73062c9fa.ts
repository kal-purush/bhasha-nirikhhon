import styled from 'styled-components'
import colors from '../../styles/colors'

export const Container = styled.div`
  display: flex;
  width: 100%;
  justify-content: center;
  padding-top: 1rem;
`

export const PreviousPage = styled.button`
  padding: 1rem 1rem 1rem 1rem;
  color: ${colors.primary};
  position: relative;

  background: transparent;
  border: 0;
  cursor: pointer;
  font-weight: 600;

  :before {
    content: '';
    position: absolute;
    width: 100%;
    height: 1px;
    top: calc(50%);
    left: -100px;
    background: ${colors.extraLightGray};
  }

  :after {
    content: '';
    position: absolute;
    width: 100%;
    height: 1px;
    top: calc(50%);
    right: -100px;
    background: ${colors.extraLightGray};
  }
`

export const NextPage = styled(PreviousPage)``