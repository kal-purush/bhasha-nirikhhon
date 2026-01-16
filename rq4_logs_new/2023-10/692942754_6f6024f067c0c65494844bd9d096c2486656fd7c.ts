import styled from "styled-components";

export const Account = styled.li`
  height: 60px;
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 8px 16px;
`;

export const Hashtag = styled.div`
  height: 60px;
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 8px 16px;
`;

export const Profile = styled.div`
  display: flex;
  align-items: center;
  overflow: hidden;
  border-radius: 100%;
`;

export const HashtagImg = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  min-width: 44px;
  min-height: 44px;
  border: 1px solid #ccc;
  border-radius: 100%;
`;

export const Info = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  height: 36px;
  font-size: 14px;
`;

export const Name = styled.span`
  padding-bottom: 3px;
`;

export const Desc = styled.span`
  color: #7c7c7c;
`;

export const Loading = styled.div`
  position: absolute;
  top: 25%;
  right: 50%;
`;