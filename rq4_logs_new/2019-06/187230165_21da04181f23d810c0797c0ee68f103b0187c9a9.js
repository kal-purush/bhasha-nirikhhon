import styled from 'styled-components';

const Wrapper = styled.div`
  position: relative;
  margin-bottom: 30px;
`;

const Input = styled.input`
  font-size: 16px;
  padding: 10px 0;
  display: block;
  color: #ffffff;
  background: transparent;
  width: 100%;
  border: none;
  border-bottom: 1px solid #ccc;
  &:focus {
    outline: none;
  }

  &:focus ~ span:before,
  &:focus ~ span:after {
    width: 50%;
  }

  &:focus ~ label,
  &:valid ~ label {
    top: -15px;
    font-size: 14px;
    color: #ffffff;
  }
`;

const Border = styled.span`
  position: relative;
  display: block;
  width: 100%;
  &::before,
  &::after {
    content: '';
    height: 2px;
    width: 0;
    bottom: 0;
    position: absolute;
    background: #ffffff;
    transition: 0.2s ease all;
  }
  &::before {
    left: 50%;
  }
  &::after {
    right: 50%;
  }
`;

const Label = styled.label`
  color: #ffffff;
  font-size: 18px;
  position: absolute;
  pointer-events: none;
  left: 10px;
  top: 15px;
  transition: 0.2s ease all;
`;

export { Wrapper, Label, Input, Border };