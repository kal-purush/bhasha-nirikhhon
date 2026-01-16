import styled, { css } from "styled-components";

export const Table = styled.table`
  border-collapse: separate;
  border-spacing: 16px 0px;
  height: 1px;
  margin-left: -16px;

  th {
    background: #3d3d3d;
    height: 64px;
    clip-path: polygon(0% 0%, 95% 0%, 100% 50%, 95% 100%, 0% 100%);
    padding: 0 16px 0 8px;
    width: 190px;

    color: #fff;
    font-size: 14px;
    text-align: center;
    font-weight: 500;

    cursor: pointer;
    transition: opacity 0.2s;

    &:hover {
      opacity: 0.9;
    }

    svg {
      background-color: #ff7802;
      stroke: #fff;
      border-radius: 10px;
      min-width: 20px;
      min-height: 20px;
      margin-right: 10px;
    }
  }

  td {
    > div {
      height: 100%;
      background: #e8e6e6;
      display: flex;
      flex-direction: column;
      padding: 8px;
      max-width: 180px;
      justify-content: center;

      > .inputContainer {
        border-bottom-color: #b7b6b6;

        button {
          right: 0;
        }

        svg {
          path {
            stroke: #b7b6b6;
          }
        }
      }
    }
  }

  .itemListing {
    display: inline-flex;

    > .custom-checkbox {
      margin-top: 0px;
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
`;

interface ItemListingTextProps {
  included: boolean;
}

export const ItemListingText = styled.h6<ItemListingTextProps>`
  color: #3d3d3d;
  font-size: 12px;
  line-height: 14px;
  margin-top: 2px;
  font-weight: 400;
  transition: color 0.2s;

  ${({ included }) =>
    included &&
    css`
      color: #ff7802;
    `}
`;