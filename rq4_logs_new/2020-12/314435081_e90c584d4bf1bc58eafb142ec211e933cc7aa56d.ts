import styled from "styled-components";
import { Modal as BSModal } from "react-bootstrap";

export const Modal = styled(BSModal)`
  .modal-dialog {
    max-width: 1200px;

    @media (min-width: 1440px) {
      max-width: 1320px;
    }

    @media (min-width: 1600px) {
      max-width: 1400px;
    }

    @media (min-width: 1900px) {
      max-width: 1700px;
    }

    @media (min-width: 2100px) {
      max-width: 1900px;
    }
  }
`;

export const Container = styled.div`
  padding: 16px 0;
  padding-right: 12px;

  > h6 {
    margin-bottom: 16px;
    text-align: center;
    color: #3d3d3d;
  }

  .inputGroup {
    display: flex;
    align-items: center;
    margin-left: 24px;

    & + .inputGroup {
      margin-top: 16px;
    }

    > .inputContainer {
      width: 370px;
      margin-left: 24px;
      border-radius: 4px;
    }

    > h6 {
      color: #3d3d3d;
      font-weight: 400;
      width: 160px;
      text-align: right;
    }
  }

  > img {
    width: 100%;
  }

  > .darkBlueButton {
    margin: 24px 0 16px 485px;
    display: block;
  }
`;