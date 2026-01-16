import styled from 'styled-components';

export const Container = styled.div`
  width: 100%;
  height: 100vh;

  display: flex;
  align-items: center;
  justify-content: center;

  div {
    display: flex;
    align-items: center;
    flex-direction: column;
  }

  h2 {
    width: 100%;
    font-size: 40px;
  }

  button {
    display: block;
    margin-top: 20px;
    padding: 5px 8px;
  }
`;