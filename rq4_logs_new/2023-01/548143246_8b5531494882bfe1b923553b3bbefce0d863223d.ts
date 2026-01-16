import styled from "@emotion/styled";

/**
 * @description styled components are used for: Home Page, 404 Page, and 500 Page.
 */

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

export const Description = styled.p`
  text-align: center;
`;

export const Margin = styled.div`
  margin: 2rem 0;
`;

export const Button = styled.button`
  border: 0.1rem solid ${(props: any) => props.theme.primary};
  border-radius: 5vh;

  padding: 0.5rem 1rem;
  margin-top: 1rem;
  color: ${(props: any) => props.theme.primary};
  background-color: ${(props: any) =>
    props.theme.id === "light" ? props.theme.secondary : "none"};

  &:hover {
    color: ${(props: any) =>
      props.theme.id === "light" ? props.theme.secondary : "none"};
    background-color: ${(props: any) =>
      props.theme.id === "light" ? props.theme.primary : "#2D2D2D"};
  }
`;