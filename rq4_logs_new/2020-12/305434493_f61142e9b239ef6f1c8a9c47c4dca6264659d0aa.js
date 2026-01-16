import React from 'react';
import Styled from "@emotion/styled";

const Wrapper = Styled('div')`
  position: absolute;
  top: 0;
  right: 0;
  padding: 10px;
  background-color: #00467f;
  background-image: url("https://www.transparenttextures.com/patterns/black-linen-2.png");
  border-radius: 0 0 0 5px;
`;

const Button = Styled('button')`
  cursor: pointer;
  background-color: #ffffff;
  border: none;
  border-radius: 5px;
  padding: 5px 10px;

  &:first-of-type {
    margin-right: 10px;
  }

  &:hover {
    background-color: #efefefef;
  }

  &:focus {
    outline: none;
  }
`;

const Save = () => {
  localStorage.setItem("log", document.getElementById('log').value)
  localStorage.setItem("notes", document.getElementById('notesarea').value)
  document.getElementById('save').textContent = "Saved"
}

const Load = () => {
  document.getElementById('log').value = localStorage.getItem("log")
  document.getElementById('notesarea').value = localStorage.getItem("notes")
  document.getElementById('load').textContent = "Loaded"
  document.getElementById('log').scrollTop = document.getElementById('log').scrollHeight;
}

function SaveButtons() {
  return (
    <Wrapper>
      <Button id="save" onClick={Save}>Save</Button>
      <Button id="load" onClick={Load}>Load</Button>
    </Wrapper>
  );
}

export default SaveButtons;