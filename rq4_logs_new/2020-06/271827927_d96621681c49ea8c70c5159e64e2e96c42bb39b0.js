import styled from "styled-components";

export const Box = styled.div`
  display: flex;
  justify-content: center;
  margin-top: 50px;

  .container {
    max-width: 933px;
    width: 100%;
  }
`;
export const Header = styled.div`
  button {
    float: right;
    box-shadow: none;
    background: #f3f5f8;
    border-radius: 3px;
    border: 2px solid #e09c57;
    color: #e09c57;
    font-weight: 600;
    cursor: pointer;
    padding: 10px 28px;
    font-size: 1rem;
    transition: 0.3s;
    outline: 0;

    &:hover {
      background: #e09c57;
      color: #fff;
    }
  }
`;

export const ListType = styled.ul`
  list-style: none;
  padding: 0;
  float: left;
  margin-top: 0;

  li {
    float: left;
    text-transform: uppercase;
    padding: 0 15px 10px;
    margin-right: 2px;
    cursor: pointer;
    color: #a5a7a6;
    font-size: 0.8rem;
    border-bottom: 3px solid #f3f5f8;
    transition: 0.3s;

    &:hover,
    &.active {
      border-color: #b92630;

      span {
        color: #b92630;
      }
    }

    p {
      margin: 0;
    }

    span {
      font-size: 1.5rem;
    }
  }
`;