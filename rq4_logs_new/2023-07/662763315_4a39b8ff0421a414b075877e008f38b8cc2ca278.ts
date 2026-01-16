'use client';
import styled from 'styled-components';

export const HeaderWrapper = styled.div`
  padding: 24px 24px 0 24px;
  background: ${({theme}) => theme.palette.dark.primary};
  border-radius: 6px 6px 0 0;
`;

export const StyledTitle = styled.h2`
  font-size: 24px;
  font-weight: 700;
  color: ${({theme}) => theme.palette.dark.secondary};
`;

export const StyledTabs = styled.ul`
  display: flex;
  gap: 24px;
`;

export const StyledTab = styled.li<{$isPath?: boolean}>`
  font-weight: 400;
  font-size: 16px;
  padding: 16px 0;
  border-bottom: 2px solid ${({theme, $isPath}) => ($isPath ? theme.palette.light.blue : 'transparent')};
  color: ${({theme, $isPath}) => ($isPath ? theme.palette.light.blue : 'inherit')};

  &:hover,
  &:focus {
    color: ${({theme}) => theme.palette.light.blue};
  }
`;