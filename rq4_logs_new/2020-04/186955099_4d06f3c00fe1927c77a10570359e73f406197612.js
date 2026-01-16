import styled from "styled-components";

const FormContainer = styled.div`
  max-width: ${props => props.theme.formWidth};
  margin: 2rem auto 0;
  padding: 0 1rem;
  @media screen and (min-width: 768px) {
    padding: 0 2rem;
  }
`;

export default FormContainer;