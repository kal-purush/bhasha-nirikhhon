import styled, { keyframes } from 'styled-components';
import { Link } from 'react-router-dom';

export const MainContainer = styled.main`
  display: flex;
  justify-content: center;
  width: 100%;
`;

export const LoginContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
  margin-top: 100px;
  max-width: 360px;
`;

export const LoginForm = styled.form`
  display: flex;
  flex-direction: column;
  justify-content: center;
  width: 100%;
  margin-top: 32px;
  gap: 16px;
`;

export const SubmitButtonStyle = styled.button`
  margin-top: 12px;
  height: 32px;
  width: 100%;
  border-radius: 8px;
  background-color: ${({ theme }) => theme.colors.mainBlue};
  border: none;
  color: white;

  ${({ theme }) => theme.fontSize.s16h24};
  font-weight: ${({ theme }) => theme.fontWeights.bold};

  &:hover {
    background-color: ${({ theme }) => theme.colors.subBlue};
  }

  &:active {
    background-color: ${({ theme }) => theme.colors.darkBlue};
    box-shadow: ${({ theme }) => theme.shadow.inset};
  }
`;

export const CustomLink = styled(Link)`
  margin-top: 12px;
  color: ${({ theme }) => theme.colors.mainBlue};
  font-size: ${({ theme }) => theme.fontSize.s14h21};
  text-decoration-line: none;
`;

export const ErrorMessage = styled.p`
  margin-top: 4px;
  padding-left: 4px;
  color: ${({ theme }) => theme.colors.paleBlue};
  font-size: 12px;
`;

export const LoadingContainer = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  position: absolute;
  left: 20px;
  top: 50%;
  transform: translate(-50%, -50%);
  width: 18px;
  height: 18px;
`;

export const spin = keyframes`
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
`;

export const Spinner = styled.div`
  width: 100%;
  height: 100%;
  border: 2px solid rgb(255 255 255 / 60%);
  border-radius: 50%;
  animation: ${spin} 1.2s linear infinite;
  border-top: 2px solid #fff;
`;