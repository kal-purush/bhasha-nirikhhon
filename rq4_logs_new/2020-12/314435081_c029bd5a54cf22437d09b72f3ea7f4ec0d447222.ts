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

  .content {
    border: 1px solid #a8a8a8;
    border-radius: 4px;
    padding: 8px;
    margin-top: 16px;
    display: flex;
    flex-direction: column;

    .inputRowGroup {
      display: flex;

      > .inputContainer:first-child {
        flex: 2;
      }
    }

    .inputContainer {
      margin-top: 8px;
    }

    > button {
      margin-top: 48px;
      align-self: flex-end;
    }
  }
`;