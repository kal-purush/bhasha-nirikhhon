import styled from "styled-components";

export const Main = styled.main`
  width: 600px;
  border-color: #71767b;
  border-right-width: 1px;
  border-left-width: 1px;
  border-left-style: solid;
  border-right-style: solid;
  display: flex;
  flex-direction: column;
  align-items: stretch;
`;

export const BackgroundImage = styled.img`
  width: 100%;
  height: 150px;
  object-fit: cover;
  position: relative;
  z-index: 1;
`;

export const Header = styled.div`
  display: flex;
  height: 53px;
  align-items: center;
`;

export const HeaderTitle = styled.h3`
  font-size: 20px;
  font-weight: bold;
  margin-left: 30px;
`;

export const UserZone = styled.div`
  display: flex;
  flex-direction: column;
  padding: 16px;
  position: relative;
  z-index: 2;
  background-color: #000;
`;

export const ProfileHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
`;

export const UserImage = styled.div`
  display: flex;
  margin-right: 12px;
  width: 134px;
  height: 134px;
  border-radius: 50%;
  overflow: hidden;
  margin-bottom: 12px;
  margin-top: -90px;

  img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 50%;
  }
`;

export const UserName = styled.div`
  margin: 0 12px;
  flex-direction: column;
  margin-bottom: 10px;

  > div:first-child {
    font-weight: bold;
    font-size: 20px;
  }

  > div:last-child {
    font-size: 15px;
    color: #71767b;
  }
`;

export const Nickname = styled.div`
  font-weight: bold;
  font-size: 20px;
`;

export const UserId = styled.div`
  font-size: 15px;
  color: #71767b;
`;

export const Message = styled.div`
  margin: 0 12px;
  margin-top: 8px;
  font-size: 14px;
  color: white;
`;

export const FollowButton = styled.button`
  border: 1px solid rgb(207, 217, 222);
  padding: 0 16px;
  border-radius: 17px;
  height: 34px;
  background-color: #000;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 15px;
  color: white;
  cursor: pointer;
  margin-left: 50%;

  &:hover {
    background-color: rgb(39, 44, 48);
  }
`;

export const PostList = styled.div`
  display: flex;
  flex-direction: column;
`;

export const UserStats = styled.div`
  display: flex;
  gap: 10px;
  margin-top: 10px;
  margin-bottom: 10px;
  margin: 12px;
`;

export const EditProfileButton = styled.button`
  border: 1px solid rgb(207, 217, 222);
  padding: 0 16px;
  border-radius: 17px;
  height: 34px;
  background-color: #000;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 15px;
  color: white;
  cursor: pointer;
  margin-left: 50%;

  &:hover {
    background-color: rgb(39, 44, 48);
  }
`;

export const StatButton = styled.button`
  color: #71767b;
  cursor: pointer;
  font-size: 15px;

  &:hover {
    text-decoration: none;
  }
`;

export const BoldText = styled.span`
  font-weight: bold;
  color: white;
`;

export const Divider = styled.div`
  width: 100%;
  height: 1px;
  background-color: #71767b;
  margin-top: 8px;
  margin-bottom: 8px;
`;