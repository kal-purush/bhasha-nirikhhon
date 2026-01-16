import styled from 'styled-components';

const Container = styled.div`
  width: 100%;
  height: 100vh;

  display: flex;
  flex-direction: column;

  ${({ theme: { breakpoints } }) => breakpoints.lg} {
    flex-direction: row;
  }
`;

export default Container;