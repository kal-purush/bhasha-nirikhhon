import styled from "styled-components";

export const Loading = styled.div`
  align-items: stretch;
  border: 0;
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  font: inherit;
  font-size: 100%;
  height: 48px;
  margin: 0;
  margin-top: 40px;
  padding: 0;
  position: relative;
  vertical-align: baseline;
`;

export const ContentArea = styled.section``;

export const ContentHeader = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
`;

export const Account = styled.div`
  box-sizing: border-box;
  display: flex;
  flex-direction: row;
  align-items: center;
  flex-grow: 1;
  flex-shrink: 1;
  max-width: calc(100% - 48px);
  height: 60px;
  padding: 14px 4px 14px 16px;
  position: relative;
  font-size: 14px;
`;

export const Profile = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  // width: 42px;
  // height: 42px;
  border-radius: 100%;
`;

export const Id = styled.span`
  margin-left: 12px;
  padding: 2px;
`;

export const FollowBtn = styled.span`
  margin-left: 18px;
  color: #0095f6;
`;

export const EtcIcon = styled.div`
  height: 40px;
  padding: 8px 16px 8px 8px;
`;

export const ImageContent = styled.div``;

export const Details = styled.div`
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  width: 100%;
  min-width: var(--media-info);
  padding: 0 16px;
  position: relative;
  font-size: 14px;
`;

export const Icons = styled.section`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
  height: 54px;
  margin-top: 4px;
  padding: 6px 0 8px;
`;

export const Left = styled.div`
  & > * {
    padding: 8px;
  }
`;

export const Right = styled.div`
  padding: 8px;
`;

export const Likes = styled.section`
  min-height: 20px;
  margin-bottom: 8px;
`;

export const PostInfos = styled.div`
  display: flex;
  flex-direction: column;
  flex-grow: 1;
  flex-shrink: 1;
  text-align: left;
  min-height: 0;
  overflow: auto;
  position: relative;
`;

export const Desc = styled.div`
  display: flex;
  gap: 5px;
  min-height: 36px;
`;

export const Writer = styled.span`
  font-weight: 600;
`;

export const PostContents = styled.span``;

export const MoreComments = styled.span`
  min-height: 18px;
  margin-bottom: 4px;
  color: #737373;
`;

export const Date = styled.div`
  margin-bottom: 8px;
  font-size: 10px;
  color: #737373;
`;