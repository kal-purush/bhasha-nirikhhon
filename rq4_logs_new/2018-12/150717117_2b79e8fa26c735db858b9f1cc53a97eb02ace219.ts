import styled, { keyframes } from '../../lib'

const loading = keyframes`
  from {
    background-position: 0 0;
  }
  to {
    background-position: 100% 100%;
  }
`

const Form = styled.form`
  width: 400px;
  padding: 20px;
  border-radius: 10px;
  border: 1px solid #01B54A;
  background: rgba(0, 0, 0, 0.02);
  font-size: 1.5rem;
  font-weight: 600;
  line-height: 1.5;
  label {
    display: block;
    margin-top: 1rem;
    margin-bottom: 0.5rem;
    font-family: 'Gotham Rounded Light';
    font-size: 0.75rem;
  }
  input,
  textarea,
  select {
    width: 100%;
    padding: 0.5rem;
    border: none;
    border-radius: 5px;
    border: 1px solid #d1d5da;
    box-shadow: inset 0 1px 2px rgba(27,31,35,.075);
    outline: none;
    background: #fcfcfc;
    font-size: 1rem;
    &:focus {
      outline: 0;
      background: ${({ theme }) => theme.white};
    }
  }
  input:focus,
  textarea:focus,
  select:focus {
    border-color: #2188ff;
  }
  div {
    display: block;
    margin: 6px 0 0;
    max-width: 450px;
    padding: 5px 8px;
    position: absolute;
    height: 30px;
    background: #ffdce0;
    border-radius: 3px;
    border: 1px solid #cea0a5;
    color: #86181d;
    font-family: 'Gotham Rounded Light';
    font-size: 0.75rem;
    z-index: 10;
  }
  div:after,
  div:before {
    bottom: 100%;
    left: 10%;
    border: solid transparent;
    content: ' ';
    height: 0;
    width: 0;
    position: absolute;
    pointer-events: none;
  }

  div:after {
    border-color: rgba(255, 220, 224, 0);
    border-bottom-color: #ffdce0;
    border-width: 6px;
    margin-left: -6px;
  }
  div:before {
    border-color: rgba(206, 160, 165, 0);
    border-bottom-color: #cea0a5;
    border-width: 7px;
    margin-left: -7px;
  }
  button,
  input[type='submit'] {
    margin-top: 2.5rem;
  }
  fieldset {
    overflow: visible;
    padding: 0;
    border: 0;
    &[disabled] {
      opacity: 0.5;
    }
    &::before {
      content: '';
      opacity: 0;
      display: block;
      height: 10px;
      background-image: linear-gradient(
        to right,
        #ff3019 0%,
        #e2b04a 50%,
        #ff3019 100%
      );
    }
    &[aria-busy='true']::before {
      opacity: 1;
      background-size: 50% auto;
      animation: ${loading} 0.5s linear infinite;
    }
  }
`

export default Form