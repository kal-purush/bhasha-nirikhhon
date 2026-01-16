import styled from "styled-components";

export const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background-color: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
`;

export const ModalContent = styled.div`
  background-color: #000;
  padding: 20px;
  border-radius: 8px;
  width: 400px;
  color: white;
`;

export const Head = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const Title = styled.h2`
  margin-top: 25px;
  font-size: 18px;
  margin-bottom: 20px;
  text-align: center;
  font-weight: bold;
`;

export const Form = styled.div`
  display: flex;
  flex-direction: column;
  gap: 10px;
`;

export const InputLabel = styled.label`
  font-size: 14px;
  color: #71767b;
`;

export const InputField = styled.input`
  padding: 10px;
  border: 1px solid #333;
  border-radius: 4px;
  background-color: #121212;
  color: white;

  &::placeholder {
    color: #71767b;
  }
`;

export const TextareaField = styled.textarea`
  padding: 10px;
  border: 1px solid #333;
  border-radius: 4px;
  background-color: #121212;
  color: white;
  resize: none;
  height: 60px;

  &::placeholder {
    color: #71767b;
  }
`;

export const SaveButton = styled.button`
  padding: 10px;
  background-color: white;
  color: black;
  font-weight: bold;
  border-radius: 35%;
  cursor: pointer;

  &:hover {
    background-color: #71767b;
  }
`;

export const DateSelectContainer = styled.div`
  display: flex;
  gap: 10px;
`;

export const Select = styled.select`
  padding: 10px;
  background-color: #121212;
  color: white;
  border: 1px solid #333;
  border-radius: 4px;
  font-size: 14px;
  width: 100vw;
  justify-content: space-between;
  text-align: center;

  &:focus {
    outline: none;
    border-color: #1da1f2;
  }
`;