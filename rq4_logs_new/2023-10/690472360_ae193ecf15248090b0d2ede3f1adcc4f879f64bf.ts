import styled from 'styled-components';
import { ReactComponent as ErrorIcon } from '../../icons/error.svg';

export const ErrorBackdrop = styled.section`
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  min-height: 257px;
  background-color: #ffffff; /* Semi-transparent background */
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 99; /* Adjust the z-index as needed to layer above other content */
`;

export const ErrorBox = styled.div`
  width: 100%;
  height: 100%;

  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  gap: 10px;

  font-family: Avenir;
  font-style: normal;
`;

export const ErrorTitle = styled.div`
  color: #4a4a4a;
  font-size: 16px;
  font-weight: 800;
  line-height: 22px;
`;

export const ErrorInfo = styled.div`
  color: #9b9b9b;
  text-align: center;
  font-size: 12px;
  font-weight: 500;
  line-height: 16px;
`;

export const MailTo = styled.a`
  color: #0fbcf9;
  font-size: 12px;
  font-weight: 500;
  line-height: 16px;
  text-decoration-line: underline;
`;

export const StyledErrorIcon = styled(ErrorIcon)``;