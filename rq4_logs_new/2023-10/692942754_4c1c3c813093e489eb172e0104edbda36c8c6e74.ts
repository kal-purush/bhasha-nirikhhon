import styled from "styled-components";

export const Header = styled.div`
  position: fixed;
  display: flex;
  flex-direction: row;
  align-items: center;
  width: 100%;
  height: 44px;
  font-size: 20px;
  font-weight: 700;
  letter-spacing: 0.3px;
  padding: 0 16px;
  border-bottom: 1px solid #ccc;
`;

export const BackIcon = styled.div`
  padding-right: 8px;
`;

export const PageTitle = styled.h1`
  display: block;
  flex-basis: 0px;
  flex-grow: 1;
  flex-shrink: 1;
  padding: 0;
  text-align: center;
  text-overflow: ellipsis;
  vertical-align: baseline;
  white-space: nowrap;
  font-size: 16px;
`;

export const Notifications = styled.div`
  position: absolute;
  top: 44px;
  width: 100%;
  margin-top: 20px;
`;

export const Date = styled.span`
  display: inline-block;
  margin: 0 12px;
  padding-bottom: 16px;
  font-size: 16px;
`;

export const ContentArea = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  height: 60px;
  padding: 8px 16px;
`;

export const Account = styled.div`
  margin-right: 14px;
  border-radius: 100%;
`;

export const Content = styled.span`
  font-size: 14px;
`;

export const Board = styled.div`
  margin-left: 14px;
`;

export const Follow = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  width: 82px;
  height: 32px;
  margin-left: 12px;
  padding: 7px 16px;
  border-radius: 10px;
  text-align: center;
  color: #fff;
  background-color: #0095f6;
`;