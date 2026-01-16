import styled from "@emotion/styled";

export const GithubButton = styled.a`
  background-color: #efefef;
  color: #000000;
  font-size: 0.875rem;
  display: flex;
  align-items: center;
  gap: 0.75rem;
  text-decoration: none;
  font-weight: 700;
  border-radius: 0.25rem;
  padding: 0.40625rem;
  align-content: center;

  @media only screen and (min-width: 640px) {
    padding: 0.40625rem 0.75rem;
  }
`;

export const GithubButtonLabel = styled.label`
  white-space: nowrap;
  display: none;

  @media only screen and (min-width: 640px) {
    display: inline;
  }
`;