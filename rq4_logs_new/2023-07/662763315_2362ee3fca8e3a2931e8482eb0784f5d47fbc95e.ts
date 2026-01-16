'use client';
import styled from 'styled-components';
import {Card} from 'antd';
import Title from 'antd/es/typography/Title';

export const StyledCard = styled(Card)`
  &.ant-card .ant-card-head {
    background: ${({theme}) => theme.palette.dark.primary};
    font-size: 24px;
    color: ${({theme}) => theme.palette.dark.secondary};
  }
  &.ant-card .ant-card-body {
    background: ${({theme}) => theme.palette.light.primary};
    border: ${({theme}) => `2px solid ${theme.palette.dark.primary}`};
    border-top: none;
    display: flex;
    justify-content: space-between;
  }
  .ant-tabs > .ant-tabs-nav .ant-tabs-nav-list {
    color: ${({theme}) => theme.palette.dark.secondary};
  }
`;

export const ContentElementWrapper = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 180px;
`;

export const ContentElementImage = styled.img`
  width: 100%;
  border-radius: 8px;
`;

export const StyledTitle = styled(Title)`
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  text-align: center;
`;