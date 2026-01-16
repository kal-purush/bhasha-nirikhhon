'use client';
import styled from 'styled-components';

export const LayoutWrapper = styled.div`
  display: flex;
  flex-direction: column;
  padding: 20px;
`;

export const LayoutHeader = styled.div`
  padding: 24px 24px 0 24px;
  background: ${({theme}) => theme.palette.dark.primary};
  border-radius: 8px 8px 0 0;
`;

export const StyledTitle = styled.h2`
  font-size: 24px;
  font-weight: 700;
  color: ${({theme}) => theme.palette.dark.secondary};
`;

export const StyledTabs = styled.ul`
  padding: 16px 0;
  display: flex;
  gap: 24px;
`;

export const StyledTab = styled.li`
  font-size: 16px;
`;

export const ActorsWrapper = styled.div`
  display: flex;
  flex-direction: column;
  width: 100%;
  padding: 20px 0;
`;

export const CardsWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-start;
  align-items: stretch;
  padding: 24px 0;
  background: ${({theme}) => theme.palette.light.primary};
  border-top: none;
  margin: -12px;
`;