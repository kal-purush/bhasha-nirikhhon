import styled from 'styled-components';

export const Container = styled.div`
  position: fixed;
  padding-top: 4rem;
  width: 100%;
  height: 100vh;
  top: 0;
  left: 0;
  background-color: rgba(255, 255, 255, 0.9);
  display: flex;
  flex-direction: column;
  align-items: center;
  z-index: 100;
`;

export const Title = styled.p`
  font-size: 1.5rem;
`;

export const ButtonBlock = styled.div`
  margin-top: 2rem;
`;