'use client';
import styled from 'styled-components';
import {Button} from 'antd';

import {Media} from '@/theme/breakpoints';

export const CardsWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-start;
  align-items: stretch;
  background: ${({theme}) => theme.palette.light.primary};
  border-top: none;
  margin: -12px;

  ${Media.down.m} {
    justify-content: center;
  }
`;

export const StyledButton = styled(Button)`
  margin: 0 auto;
  max-width: 300px;
  display: block;
`;

export const LoaderWrapper = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
`;