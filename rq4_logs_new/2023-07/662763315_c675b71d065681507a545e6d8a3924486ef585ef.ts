'use client';
import styled from 'styled-components';
import {Button} from 'antd';

// import {Media} from '@/theme/breakpoints';

export const CardsWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-start;
  align-items: stretch;
  background: ${({theme}) => theme.palette.light.primary};
  border-top: none;
  margin: -12px;
`;

export const CardWrapper = styled.div`
  width: 220px;
  margin: 12px;
  border: 2px solid ${({theme}) => theme.palette.light.secondary};
  border-radius: 8px;
  cursor: pointer;
  padding: 4px;
  transition: all 500ms ease;

  &:hover,
  &:focus {
    transform: scale(1.02);
    box-shadow: rgba(50, 50, 93, 0.25) 0 50px 100px -20px, rgba(0, 0, 0, 0.3) 0px 30px 60px -30px,
      rgba(10, 37, 64, 0.35) 0px -2px 6px 0px inset;
  }
`;

export const ImageWrapper = styled.div`
  overflow: hidden;
  border-radius: 8px;
  width: 100%;
`;

export const ContentWrapper = styled.div`
  margin-top: 8px;
  border-radius: 8px;
  padding: 0 8px;
`;

export const StyledTitle = styled.h3`
  text-align: center;
  margin-bottom: 8px;
  font-size: 20px;
  font-weight: 700;
`;

export const StyledText = styled.h6`
  font-size: 14px;
  font-weight: 700;
  color: ${({theme}) => theme.palette.dark.secondary};
  margin-right: 10px;
`;
export const StyledTextItem = styled.p`
  font-size: 14px;
  font-weight: 400;
  color: ${({theme}) => theme.palette.dark.secondary};
`;

export const StyledButton = styled(Button)`
  margin: 24px auto 0;
  width: 200px;
  display: block;
`;

export const InnerCardContentWrapper = styled.div`
  display: flex;
  align-items: center;
  gap: 4px;
  margin-top: 4px;
`;

export const AboutWrapper = styled.div`
  max-width: 800px;
`;

export const AboutItemWrapper = styled.div`
  display: flex;
  gap: 4px;
  margin-top: 8px;
`;

export const AboutItemsWrapper = styled.div`
  display: flex;
  flex-direction: column;
  gap: 4px;
`;
export const StyledLink = styled.a`
  color: ${({theme}) => theme.palette.light.blue};
  font-size: 14px;
  font-weight: 400;
  text-decoration: none;
  transition: all 500ms ease;

  &:hover {
    color: ${({theme}) => theme.palette.dark.secondary};
    text-decoration: underline;
  }
`;