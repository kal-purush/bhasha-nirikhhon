import styled, { css } from 'styled-components';
import Common from '../common/styled';
import FDKButton from '../fdk-button';

const StatusBar = styled.div`
  display: flex;
  justify-content: center;
  position: fixed;
  left: 0;
  right: 0;
  bottom: 0;
  height: 64px;
  background: ${({ theme }) => theme.fdk.colors.neutrals.skyblue};
  border-top: 1px solid ${({ theme }) => theme.fdk.colors.neutrals.lightblue};
`;

const StatusBarBody = styled(Common.Container)`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const ButtonGroup = styled.div`
  display: flex;
  align-items: center;
`;

const StatusButton = styled(FDKButton)`
  border-radius: 0;

  &:first-of-type {
    border-radius: 3px 0 0 3px;
  }

  &:last-of-type {
    border-radius: 0 3px 3px 0;
  }
`;

const RemoveButton = styled.button<{ disabled?: boolean }>`
  margin-left: 8px;
  color: ${({ theme }) => theme.fdk.colors.text.link};

  &:hover {
    text-decoration: underline;
  }

  ${({ disabled }) =>
    disabled &&
    css`
      opacity: 0.5;
      pointer-events: none;
      cursor: not-allowed;

      &:hover {
        text-decoration: none;
      }
    `}
`;

const ConfirmButton = styled(FDKButton)``;

const CancelButton = styled(RemoveButton)``;

export default {
  StatusBar,
  StatusBarBody,
  ButtonGroup,
  StatusButton,
  RemoveButton,
  ConfirmButton,
  CancelButton
};