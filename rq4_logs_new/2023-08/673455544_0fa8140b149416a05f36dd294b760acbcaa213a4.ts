import { styled } from 'styled-components';

export const ContainerSlider = styled.section`
  position: relative;
  height: 100%;

  &::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0, 0, 0, 0.6); // Cor escura com opacidade
  }

  @media (min-width: 768px) {
    height: 34rem;
  }
`

export const ImgSlider = styled.img`
  width: 100%;
  height: 100%;


  position: relative;
  z-index: 1;

  @media (min-width: 768px) {
    height: 34rem;
    object-fit: cover;
  }
`
