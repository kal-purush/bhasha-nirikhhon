import styled from "styled-components";

export const PageWrapper = styled.div`
  max-width: 480px;
  min-width: 320px;
  height: 100vh;

  margin: 0 auto;
  padding: 20px;

  background-color: ${({ theme }) => theme.colors.background.secondary};
  opacity: 0.9;
`;

export const CenterWrapper = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;

  height: 100%;
`;