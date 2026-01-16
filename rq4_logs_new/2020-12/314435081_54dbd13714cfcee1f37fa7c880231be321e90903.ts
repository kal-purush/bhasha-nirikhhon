import styled from "styled-components";
import { Modal as BSModal } from "react-bootstrap";
import colors from "assets/colors";
import { shade } from "polished";

export const Modal = styled(BSModal)`
  .modal-dialog {
    max-width: 950px;
  }

  &.modal {
    z-index: 1070;
  }
`;

export const Container = styled.div`
  padding: 32px 24px;

  > .borderedContainer {
    border: 1px solid #a8a8a8;
    border-radius: 4px;
    padding: 24px 16px 96px;
    position: relative;
    display: flex;
    flex-direction: row;
    min-height: 400px;

    > label {
      color: #18325c;
      font-size: 21px;
      line-height: 24px;
      margin-bottom: 0px;
      position: absolute;
      top: -16px;
      left: 16px;
      padding: 0 4px;
      background-color: #fff;
    }
  }
`;

export const MapAndAddressListContainer = styled.div`
  > div:last-child {
    margin-top: 24px;
    border-top: 1px solid #a8a8a8;
    position: relative;
    padding-top: 16px;

    > label {
      color: #000;
      font-size: 10px;
      line-height: 12px;
      margin-bottom: 0px;
      position: absolute;
      top: -8px;
      left: 16px;
      padding: 0 4px;
      background-color: #fff;
    }
  }

  > .attributeListContainer {
    small {
      background-color: #fff;
    }
  }
`;

export const AddLocationContainer = styled.div`
  flex: 1;
  padding-left: 8px;

  .inputContainer {
    border-bottom-color: #a8a8a8;
  }

  > main {
    padding-left: 8px;

    > label {
      color: #3d3d3d;
      font-size: 14px;
      margin-top: 40px;
      margin-bottom: 0px;
    }

    span {
      color: #3d3d3d;
    }

    input {
      color: #3d3d3d;
    }

    .inputContainer {
      margin-top: 12px;

      span {
        font-weight: 600;
      }

      input {
        flex: 1;
      }
    }

    .latLongInputGroup {
      display: flex;
      margin-top: 16px;

      .inputContainer {
        flex: 1;
      }
    }

    .buttonsContainer {
      display: flex;
      justify-content: space-between;
      margin-top: 24px;

      button:first-child {
        background-color: ${colors.darkBlue};
        font-weight: 500;
        margin-right: 16px;
        border-color: ${colors.darkBlue};

        &:hover {
          background-color: ${shade(0.2, colors.darkBlue)};
        }

        &:active {
          background-color: ${shade(0.3, colors.darkBlue)};
        }
      }
    }
  }
`;