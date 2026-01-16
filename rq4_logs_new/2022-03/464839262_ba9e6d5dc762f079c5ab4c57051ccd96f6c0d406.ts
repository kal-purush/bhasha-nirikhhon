import styled from 'styled-components';

export const SwitchContainer = styled.div`
  font-size: 0;

  input {
    height: 0;
    width: 0;

    opacity: 0;
    visibility: hidden;
  }

  label {
    position: relative;
    width: 100px;
    height: 50px;

    display: flex;
    align-items: center;
    justify-content: space-between;

    cursor: pointer;
    background: grey;
    border-radius: 100px;
    transition: background-color 0.2s;
  }

  span {
    content: '';
    position: absolute;
    top: 2px;
    left: 2px;
    width: 45px;
    height: 45px;

    border-radius: 45px;
    background: #fff;
    box-shadow: 0 0 2px 0 rgba(10, 10, 10, 0.29);
    transition: 0.2s;
  }

  label:active span {
    width: 60px;
  }

  input:checked + label span {
    left: calc(100% - 2px);
    transform: translateX(-100%);
  }
`;