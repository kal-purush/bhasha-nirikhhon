import styled from "styled-components";

export const NotFoundContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;

  transform: translateY(-200px);
  margin: 50% auto 0;
  h1 {
    font-size: 10rem;
    font-weight: 900;
    text-transform: uppercase;
    color: ${(props) => props.theme["red-300"]};
    padding: 0;
    margin: 0;
    line-height: 1;
    text-align: center;
  }

  span {
    font-weight: bold;
    color: ${(props) => props.theme["gray-100"]};
  }

  a {
    display: inline-flex;
    margin-block: 16px;
    font-size: 0.75rem;
    text-decoration: none;
    color: ${(props) => props.theme["gray-300"]};

    &:hover {
      color: ${(props) => props.theme["gray-500"]};
    }
  }
`;