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
  font-size: 1.3rem;
  color: black;
  font-weight: lighter;
  margin-left: 45%;
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
  padding: 32px 0 0 20px;
`;

export const ProfileCont = styled.div`
  width: 100%;
  min-height: 21px;
  margin-top: 40px;
`;

export const H2 = styled.h2`
  width: 100%;
  font-size: 2rem;
  height: 30px;
  font-weight: lighter;
`;

export const MyProfile = styled.div`
  display: flex;
  align-items: center;
`;

export const ProfileImage = styled.div`
  width: 38px;
  height: 38px;
  background-color: green;
  border-radius: 50%;
  margin-right: 10px;
`;

export const IdCont = styled.div`
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
`;

export const MyId = styled.span`
  display: flex;
  flex-direction: column;
  font-size: 1.3rem;
  margin-bottom: 5px;
`;

export const ChangeProfile = styled.span`
  font-size: 1.4rem;
  color: #0095f6;
`;

export const IntroduceCont = styled.div`
  width: 100%;
  min-height: 114px;
`;

export const Aside = styled.aside`
  font-weight: bold;
  font-size: 1.2rem;
  text-align: bottom;
  padding: 2rem 0 1rem 0;
`;

export const TextArea = styled.textarea`
  width: 90%;
  min-height: 100px;
  border-radius: 8px;
  border: 1px solid #ccc;
`;

export const Dropdown = styled.select`
  min-width: 20%;
  min-height: 40px;
  border-radius: 8px;
`;

export const PrivatePolicy = styled.aside`
  width: 100%;
  color: #737373;
  padding: 1rem 0 1rem 0;
`;

export const RecommendCont = styled.div`
  min-height: 90px;
`;

export const RecommendMyID = styled.aside`
  padding: 1rem 0 1rem 0;
  font-weight: bold;
  font-size: 1.2rem;
`;
export const CheckboxCont = styled.div`
  display: flex;
  align-items: center;
`;

export const Checkbox = styled.input`
  width: 15px;
  height: 15px;
`;

export const CheckboxDesc = styled.span`
  padding-left: 1rem;
  font-size: 1.2rem;
  line-height: 1.5rem;
`;

export const SubmitCont = styled.div`
  width: 100%;
  min-height: 40px;
  padding-top: 2rem;
`;

export const Submit = styled.button`
  background-color: #0095f6;
  color: white;
  font-size: 1.3rem;
  width: 20%;
  height: 40px;
  border: none;
  border-radius: 5px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
`;