import styled from 'styled-components'

export const FormInput = styled.input`
  color: --text-color;
  padding: 0.7rem 1.7rem;
  margin: 5px;
  border-radius: 10px;
`
export const SiteForm = styled.form`
  color: --text-color;
  display: flex;
  width: 500px;
  margin: 5rem auto;
  flex-direction: column;
  gap: 10px;
`;

export const FormLabel  = styled.label`
  display: flex;
  flex-direction: column;
  gap: 10px;
`
export const FormButton = styled.button`
  color: --text-color;
  padding: 0.5rem 1rem;
  border: 0;
  border-radius: 5px;
  font-size: 17px;
  &:hover {
    opacity: .7;
  }
  &:disabled{
    opacity: .2;
  }
`