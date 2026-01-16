import styled from 'styled-components';

export const Item = styled.div`
  width: 50px;
  height: 50px;
  background: none;
  display: flex;
  justify-content: center;
  align-items: center;
  border-radius: 50%;
  transition: all .1s ease-in-out;
  .link{
    font-size: 28px;
    transition: all .1s ease-in-out;
  }
  &:hover{
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    transform: scale(1.1);
  }
  &:hover .info{
    display: flex;
  }
`