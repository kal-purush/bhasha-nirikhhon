import styled from "styled-components";

export const Container = styled.div`
  padding: 16px;

  > h6 {
    margin-bottom: 24px;
    text-align: center;
    color: #3d3d3d;
  }

  .inputGroup {
    display: flex;
    align-items: center;

    & + .inputGroup {
      margin-top: 16px;
    }

    > .inputContainer {
      margin-left: 16px;
      border-radius: 4px;
      flex: 1;
    }

    > h6 {
      color: #3d3d3d;
      font-weight: 400;
      width: 165px;
      text-align: right;
    }
  }

  > .darkBlueButton {
    margin-top: 24px;
    display: block;
    margin-left: auto;
  }
`;