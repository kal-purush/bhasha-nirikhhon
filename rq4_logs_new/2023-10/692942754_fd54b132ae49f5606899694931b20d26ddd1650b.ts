import styled from "styled-components";

export const Header = styled.div`
  width: 412px;
  height: 44px;
  align-items: center;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  padding: 0 16px;
  position: fixed;
  background-color: #ffffff;
  color: #222222;
  z-index: 10;
  border-bottom: 1px solid #e2e2e2;
`;

export const H1 = styled.h1`
  font-size: 1.4rem;
  color: black;
  font-weight: lighter;
  margin-left: 25%;
`;

export const Prev = styled.button`
  width: 35px;
  height: 35px;
  background-color: transparent;
  border-style: none;
  font-size: 1.3rem;
  display: flex;
  align-items: center;
  padding: 0;
  position: absolute; // 추가
  left: 5%; // 추가
`;

export const Container = styled.section`
  width: 100%;
  min-height: 100vh;
  background-color: white;
  color: black;
  overflow-y: scroll;
  display: flex;
  flex-direction: column;
`;

export const RecommendCont = styled.div`
  width: 100px;
  font-size: 1.5rem;
  margin-top: 30px;
  padding: 32px 0 0 20px;
`;

export const Recommend = styled.aside`
  width: 100%;
  font-size: 1.5rem;
`;

export const UserCont = styled.div`
  min-width: 412px;
  height: 70px;
  padding: 3rem 0 3rem 0;
  display: flex;
  align-items: center;
`;

export const UserProfile = styled.div`
  width: 50px;
  height: 45px;
  border-radius: 50%;
  background-color: pink;
`;

export const UserStatus = styled.div`
  width: 60%;
  height: 50px;
  display: flex;
  flex-direction: column;
  padding: 0 1rem 0 1rem;
  justify-content: center;
`;

export const UserId = styled.div`
  color: black;
  letter-spacing: 0.3px;
`;

export const UserIntro = styled.div`
  color: #737373;
  font-size: 1.3rem;
  line-height: 1.7rem;
`;

export const UserFollower = styled.div`
  color: #737373;
  font-size: 1.3rem;
`;

export const FollowButton = styled.button`
  width: 80px;
  height: 35px;
  background-color: #3897f0;
  border: none;
  color: #ffffff;
  padding: 8px 16px;
  font-size: 1.4rem;
  border-radius: 4px;
  cursor: pointer;
  &.following {
    background-color: #ccc;
    color: black;
  }
`;