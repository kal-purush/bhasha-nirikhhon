import styled, { css } from "styled-components";
import { Modal as BSModal } from "react-bootstrap";
import { shade } from "polished";

export const Modal = styled(BSModal)`
  .modal-dialog {
    max-width: 850px;
  }

  &.modal {
    z-index: 1060;
  }
`;

export const Container = styled.div`
  max-width: 850px;
  padding: 32px 16px;

  .borderedContainer {
    border: 1px solid #a8a8a8;
    border-radius: 4px;
    padding: 24px 16px 96px;
    position: relative;
    display: flex;
    flex-direction: row;
    min-height: 400px;

    > .darkBlueButton {
      position: absolute;
      bottom: 8px;
      left: 0;
      right: 0;
      margin: 0 auto;
      font-weight: 500;
      height: 48px;
      width: 152px;
      font-size: 18px;
    }

    & + .borderedContainer {
      margin-top: 24px;
    }

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

    /* lado esquerdo, container dos 3 inputs */
    > div:nth-child(2) {
      flex: 3;
      padding-right: 16px;

      .inputContainer + .inputContainer {
        margin-top: 16px;
      }
    }

    > div:nth-child(3) {
      flex: 2;
      padding-left: 16px;
    }
  }

  .resourceListing {
    margin-top: 24px;
    position: relative;
    padding: 32px 16px 16px;

    > h5 {
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      text-align: center;
    }

    > .inputContainer {
      position: absolute;
      right: 0;
      top: 0;
      border-bottom-color: #8d8d8d;

      svg {
        path {
          stroke: #8d8d8d;
        }
      }
    }

    .resourceListItem {
      padding: 8px 2px;
      border-bottom: 1px solid #9f9f9f;
      border-top: 1px solid #9f9f9f;

      & + .resourceListItem {
        border-top: none;
      }

      > div {
        display: flex;
        align-items: flex-start;
        margin-top: 6px;

        > h6 {
          min-width: 200px;
          font-size: 14px;
          line-height: 16px;
          color: #555656;
          font-weight: 700;
        }

        > .contactAttributeContainer {
          line-height: normal;
          > span {
            & + span {
              margin-top: 6px;
            }
          }
        }

        span {
          font-size: 14px;
          line-height: 16px;
          color: #555656;
          display: block;
        }
      }
    }
  }
`;

interface Props {
  selected: boolean;
}

export const ResourceAccordionItem = styled.div<Props>`
  & + & {
    margin-top: 4px;
  }

  header {
    background-color: rgba(196, 196, 196, 0.25);
    height: 48px;
    display: flex;
    justify-content: center;
    align-items: center;
    cursor: pointer;
    transition: background-color 0.2s;

    ${({ selected }) =>
      selected &&
      css`
        background-color: #c4c4c4;
      `}

    h5 {
      color: #18325c;
      font-weight: 400;
    }
  }

  .accordionItemContent {
    padding: 0 4px 8px;
  }

  .contactContent {
    padding-top: 8px;

    .attributeListContainer {
      small {
        background-color: #fff;
      }
    }
  }

  .addressContent {
    > .inputContainer {
      > span {
        color: #212121;
      }
      svg {
        path {
          stroke: #8d8d8d;
        }
      }
    }

    .form-check {
      & + .form-check {
        margin-top: 8px;
      }

      label {
        color: #212121;
        font-size: 12px;
        line-height: 14px;
      }
    }

    > .darkBlueButton {
      margin-left: auto;
      display: block;
    }
  }
`;