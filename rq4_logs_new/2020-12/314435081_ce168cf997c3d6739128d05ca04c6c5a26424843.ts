import styled from "styled-components";
import { Modal as BSModal } from "react-bootstrap";
import colors from "assets/colors";
import { shade } from "polished";

export const Modal = styled(BSModal)`
  .modal-dialog {
    max-width: 850px;
  }
`;

export const Container = styled.div`
  width: 850px;
  border-radius: 4px;
  position: relative;
  padding: 4px;
  padding-bottom: 32px;

  .darkBlueButton {
    position: absolute;
    bottom: 12px;
    right: 29px;
  }

  > header {
    display: flex;
    background-color: ${colors.darkBlue};
    height: 80px;
    position: absolute;
    left: 0;
    top: 0;
    right: 0;
    border-radius: 4px 4px 0 0;

    button {
      display: flex;
      flex: 1;
      align-items: center;
      justify-content: center;
      position: relative;

      &:hover {
        opacity: 0.6;
      }

      & + button {
        border-left: 1px solid #a8a8a8;
      }

      span {
        font-size: 14px;
        color: #ffffff;
        margin-left: 12px;
      }
    }
  }

  > div {
    background-color: #fff;
    margin-top: 80px;
    padding: 32px 28px;

    > button {
      background-color: ${colors.darkGrey} !important;
      border-color: ${colors.darkGrey} !important;

      &:hover {
        background-color: ${shade(0.1, colors.darkGrey)} !important;
      }
    }
  }
`;

export const MembersContainer = styled.div`
  margin-top: -1px;
  margin-left: 3px;
  border-top-right-radius: 4px;
  padding-top: 48px;

  display: flex;
  width: 100%;
  border-width: 1px 1px 1px 0px;
  border-color: #000;
  border-style: solid;

  flex-direction: column;

  .memberFilter {
    width: 200px;
    margin-bottom: 16px;
    margin-left: auto;
    margin-right: 16px;

    border-color: #000;

    > input {
      color: #3d3d3d;
    }

    > span {
      color: #3d3d3d;
    }

    > svg {
      path {
        stroke-width: 3px;
      }
    }
  }

  > table {
    margin-left: -24px;

    th {
      background-color: #555656;
      color: #fff;
      padding: 4px 10px 4px 10px;
      position: relative;

      &:not(:last-child) {
        border-right: 1px solid #000;
      }

      &:first-child {
        border-bottom: none;
        background-color: #fff;
        padding: 0;
        padding-right: 5px;
      }

      /* Ordem */
      &:nth-child(2) {
        text-align: center;
        padding: 4px;
        width: 58px;
      }

      /* Nome */
      &:nth-child(3) {
        width: 251px;
      }

      /* Função */
      &:nth-child(4) {
        width: 279px;
      }

      /* Telefone */
      &:nth-child(5) {
        width: 197px;
      }

      > svg {
        position: absolute;
        right: 10px;
      }
    }

    tr:not(:last-child) {
      td {
        border-bottom: 1px solid #000;
      }
    }

    td {
      padding: 4px 10px;
      border-right: 1px solid #000;
      color: #424242;
      position: relative;

      &:first-child {
        border-bottom: none !important;
        padding: 0;
      }

      &:nth-child(2) {
        text-align: center;
        padding: 4px;
      }

      > span {
        height: 12px;
        width: 12px;
        border-radius: 12px;
        background-color: #ff0000;
        position: absolute;
        right: 10px;
        top: 10px;
      }
    }
  }
`;