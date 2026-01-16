import styled from "styled-components";

import { Modal as BSModal } from "react-bootstrap";

export const Modal = styled(BSModal)`
  .modal-dialog {
    max-width: 610px;
  }

  &.modal {
    z-index: 1060;
  }
`;

export const Container = styled.div`
  width: 610px;
  background-color: #f0efec;
  border-radius: 4px;
  position: relative;
  padding: 24px 16px;
  height: 436px;

  > h6 {
    text-align: center;
    color: #3d3d3d;
    margin-bottom: 40px;
  }

  > img {
    width: 70px;
    height: 75px;
    position: absolute;
    top: 8px;
    right: 24px;
  }

  .userNotFoundContainer {
    background: #cecece;
    padding: 8px;
    border-radius: 4px;
    margin-top: 16px;

    small {
      text-align: center;
      color: #3d3d3d;
      font-size: 14px;
      display: block;
    }

    > div {
      display: flex;
      justify-content: space-between;
      margin-top: 8px;
    }
  }

  .userFoundContainer {
    border: 1px solid #a8a8a8;
    border-radius: 4px;
    padding: 8px;
    margin-top: 16px;
    display: flex;
    flex-direction: column;

    small {
      text-align: center;
      color: #3d3d3d;
      font-size: 14px;
      display: block;
    }

    .inputRowGroup {
      display: flex;

      > .foundUserInput:first-child {
        flex: 2;
      }
    }

    .foundUserInput {
      height: 24px;
      margin-top: 8px;
    }

    > button {
      margin-top: 48px;
      align-self: flex-end;
    }
  }
`;