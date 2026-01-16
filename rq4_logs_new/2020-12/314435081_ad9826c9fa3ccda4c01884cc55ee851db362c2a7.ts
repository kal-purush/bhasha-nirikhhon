import styled from "styled-components";

export const Container = styled.div`
  &.scenarioColumn {
    max-width: 200px;
    min-width: 160px;

    & + .scenarioColumn {
      margin-left: 24px;
    }

    > button {
      padding: 0;
      width: 105%;

      &:hover {
        opacity: 0.9;
      }

      header {
        background: #3d3d3d;
        display: flex;
        justify-content: center;
        align-items: center;
        min-height: 64px;
        clip-path: polygon(0% 0%, 95% 0%, 100% 50%, 95% 100%, 0% 100%);
        padding: 0 16px 0 8px;

        svg {
          background-color: #ff7802;
          stroke: #fff;
          border-radius: 10px;
          min-width: 20px;
          min-height: 20px;
          margin-right: 10px;
        }

        > h6 {
          color: #fff;
          font-size: 14px;
          text-align: center;
          white-space: normal;
        }
      }
    }

    > main {
      background: #e8e6e6;
      height: 100%;
      min-height: 250px;
      display: flex;
      flex-direction: column;
      padding: 8px;

      > .inputContainer {
        border-bottom-color: #b7b6b6;
        svg {
          path {
            stroke: #b7b6b6;
          }
        }
      }

      .itemListing {
        display: inline-flex;

        > .custom-checkbox {
          margin-top: 14px;
          margin-right: -4px;

          input {
            z-index: 1;
            height: 10px;
            width: 10px;
            top: 4px;
          }

          .custom-control-label::before {
            background-color: transparent;
            border-color: #4f4f4f;
            border-radius: 0;
            height: 10px;
            width: 10px;
          }

          .custom-control-input:checked ~ .custom-control-label {
            &::after {
              left: -25px;
              height: 12px;
              width: 12px;
              background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' width='8' height='8' viewBox='0 0 8 8'%3e%3cpath fill='%234f4f4f' d='M6.564.75l-3.59 3.612-1.538-1.55L0 4.26l2.974 2.99L8 2.193z'/%3e%3c/svg%3e");
              top: 3px;
            }

            &::before {
              background-color: transparent;
              border-color: #4f4f4f;
            }
          }
        }
      }
    }
  }
`;