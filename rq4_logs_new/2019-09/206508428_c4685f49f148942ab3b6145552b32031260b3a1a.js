import styled from 'styled-components';
import { fonts, colors } from '../../helpers/globalTheme';

const StyledResults = styled.div`
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  width: 100vw;
  height: 100vh;
  z-index: 200;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background: ${colors.isLight.background};
  font-family: ${fonts.secondary};
  h1 {
    text-align: center;
    color: ${colors.gameBoy.primary};
    text-shadow: 4px 4px 0px ${colors.gameBoy.secondary};
  }
  h3 span {
    text-align: center;
    color: ${colors.primary};
    text-shadow: 4px 4px 0px ${colors.secondary};
  }
`;

export default StyledResults;