import styled from 'styled-components';
import { colors } from '@styles/theme/colors';

import postEditIcon from '@assets/svgs/PostEditIcon.svg?react';
import postDeleteIcon from '@assets/svgs/PostDeleteIcon.svg?react';

export const DropdownContainer = styled.div`
  position: absolute;
  top: 3.8rem;
  right: 25px;

  width: 7.8rem;
  height: 6.2rem;
  padding: 0.9rem 1rem;

  display: flex;
  flex-direction: column;
  justify-content: space-around;
  align-items: center;

  background-color: ${colors.TZ_Monochrome[0]};
  border-radius: 6px;
  box-shadow: 0px 0px 4px 0px rgba(150, 150, 150, 0.25);

  z-index: 100;
`;

export const OptionsContainer = styled.div`
  width: 100%;

  display: flex;
  justify-content: space-between;
  align-items: center;

  cursor: pointer;

  span {
    ${({ theme }) => theme.fontStyles.Caption7}
    color: ${colors.TZ_Monochrome[1000]};
  }
`;

export const EditIcon = styled(postEditIcon)`
  width: 1rem;
  height: 1rem;
`;

export const DeleteIcon = styled(postDeleteIcon)`
  width: 1rem;
  height: 1rem;
`;