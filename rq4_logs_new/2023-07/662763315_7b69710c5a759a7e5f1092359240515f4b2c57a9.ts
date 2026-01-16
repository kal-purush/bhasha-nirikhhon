'use client';
import styled from 'styled-components';

export const HeaderWrapper = styled.header`
  width: 100%;
  background: ${({theme}) => theme.palette.dark.secondary};
`;

export const HeaderContent = styled.div`
  min-height: 100px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  color: ${({theme}) => theme.palette.light.primary};
`;

export const FooterWrapper = styled.footer`
  width: 100%;
  background: ${({theme}) => theme.palette.dark.secondary};
  padding: 0 20px;
`;

export const FooterContent = styled.div`
  min-height: 100px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: ${({theme}) => theme.palette.light.primary};
`;