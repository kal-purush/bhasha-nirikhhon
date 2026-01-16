import { styled, theme } from '../../../theme';
import {
  DeleteMovieContainerProps,
  CrossItemProps,
} from './DeleteMovie.interface';
import { ButtonPink } from '../../common/button/Button.style';

export const DeleteMovieContainer = styled.div<DeleteMovieContainerProps>`
  display: ${(p) => (p.open === false ? 'none' : 'block')};
  position: fixed;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: ${theme.colors.black};
  z-index: 1;
`;

export const DeleteMovieForm = styled.form`
  position: relative;
  max-width: 580px;
  margin: 3vh auto 0 auto;
  padding: 50px 60px;
  border-radius: 6px;
  background-color: ${theme.colors.black};
  overflow-y: scroll;
  border: 3px solid ${theme.colors.pink};
`;

export const DeleteMessage = styled.p`
  color: ${theme.colors.white};
  margin-top: 40px;
  font-weight: 400;
  font-size: 18px;
`;

export const CrossItem = styled.div<CrossItemProps>`
  & > svg {
    position: absolute;
    right: 45px;
    width: 20px;
    fill: ${theme.colors.white};
    &:hover {
      cursor: pointer;
      fill: ${theme.colors.pink};
    }
  }
`;

export const ConfirmButton = styled(ButtonPink)`
  padding: 15px 40px;
  float: right;
  margin-right: 20px;
`;