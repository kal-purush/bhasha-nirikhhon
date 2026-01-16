import Image from "next/image";
import styled, { css } from "styled-components";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

export const PostImage = styled(Image)``;

export const PostDetails = styled.section`
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  width: 100%;
  min-width: var(--media-info);
  padding: 0 16px;
  position: relative;
  font-size: 14px;
`;

export const IconArea = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
  height: 54px;
  margin-top: 4px;
  padding: 6px 0 8px;
`;

export const Left = styled.div`
  display: flex;
  flex-direction: row;

  & > * {
    padding: 8px;
  }
`;

export const Right = styled.div`
  padding: 8px;
`;

const Icons = css`
  font-size: 24px;
`;

export const CommentIcon = styled(FontAwesomeIcon)`
  ${Icons}
`;

export const DmIcon = styled(FontAwesomeIcon)`
  ${Icons}
`;

export const LikeIcon = styled(FontAwesomeIcon)`
  ${Icons}
`;

export const BoomMarkIcon = styled(FontAwesomeIcon)`
  ${Icons}
`;

export const LikesArea = styled.section`
  margin-bottom: 14px;
`;

export const InfosArea = styled.div`
  position: relative;
  display: flex;
  flex-direction: column;
  flex-grow: 1;
  flex-shrink: 1;
  text-align: left;
  overflow: auto;
`;

export const Infos = styled.div`
  display: flex;
  margin-bottom: 20px;
`;

export const Writer = styled.span`
  padding-right: 5px;
  font-weight: 600;
`;

export const Contents = styled.span``;

export const MoreComments = styled.button`
  display: flex;
  margin-bottom: 8px;
  border: none;
  font-size: inherit;
  color: #737373;
  background-color: transparent;
`;

export const CreatedAt = styled.span`
  margin-bottom: 8px;
  font-size: 10px;
  color: #737373;
`;