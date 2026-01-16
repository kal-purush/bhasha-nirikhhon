import styled from "styled-components";

export const SearchContainer = styled.div`
  display: flex;
  margin-block: 30px;
  width: 100%;
  justify-content: center;
`;
export const LabelSearchContainer = styled.label`
  display: flex;
  align-items: center;
  position: relative;
  color: ${(props) => props.theme["red-300"]};
`;
export const SearchInputContainer = styled.input`
  width: 50rem;
  background: ${(props) => props.theme["red-100"]};
  padding: 20px;
  border: 0;
  border-radius: 80px;
  color: ${(props) => props.theme["red-300"]};
  font-weight: bold;
  font-size: 0.875rem;
  padding-left: 80px;

  &::placeholder,
  &::-webkit-input-placeholder {
    color: ${(props) => props.theme["red-300"]};
  }
  &:-ms-input-placeholder {
    color: ${(props) => props.theme["red-300"]};
  }
`;
export const SearchInputIcon = styled.img`
  height: 32px;
  width: 32px;
  position: absolute;
  left: 30px;
`;