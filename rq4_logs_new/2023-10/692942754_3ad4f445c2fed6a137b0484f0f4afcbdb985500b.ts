import styled from "styled-components";

export const Container = styled.div`
  width: 100%;
  height: 915px;
  background-color: aliceblue;
`;

export const Notification = styled.div`
  width: 412px;
  height: 70vh;
  background-color: antiquewhite;
  padding-bottom: 60px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
`;

export const Title = styled.h1`
  width: 50%;
  font-size: 3rem;
  text-align: center;
  margin: 2rem 0 2rem 0;
`;

export const LoginCont = styled.div`
  width: 80%;
  height: 27vh;
  padding-bottom: 2rem;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

export const Id = styled.input`
  width: 205px;
  height: 40px;
  margin-bottom: 1.5rem;
  border-radius: 8px;
  border-style: none;
  background-color: whitesmoke;
  padding-left: 5px;
`;

export const Password = styled.input`
  width: 205px;
  height: 40px;
  border-radius: 8px;
  border-style: none;
  background-color: whitesmoke;
  padding-left: 5px;
`;

export const DownButton = styled.button`
  width: 332px;
  height: 44px;
  border-radius: 8px;
  background: #0095f6;
  color: white;
  border-style: none;
`;

export const LoginButton = styled.button`
  width: 332px;
  height: 44px;
  border-radius: 8px;
  background: white;
  color: #737373;
  border-style: none;
  margin-top: 2rem;
`;

export const FindContainer = styled.div`
  width: 100%;
  height: 35px;
`;

export const FindPassword = styled.span`
  color: #0095f6;
  padding: 1rem 0 1rem 5rem;
  margin: 1rem;
  font-size: 1.3rem;
`;

export const Login = styled.button`
  width: 80%;
  height: 35px;
  border-radius: 8px;
  background-color: #0095f6;
  border-style: none;
  color: white;
  padding: 1rem;
  text-align: center;
  font-size: 1.3rem;
`;

export const NotaMember = styled.span`
  color: #737373;
  font-size: 1.3rem;
`;

export const SignIn = styled.span`
  color: #0095f6;
  font-weight: bold;
  margin-left: 5px;
`;