import { Link } from "react-router-dom";
import { styled } from "styled-components";

export const WelcomeMainContainer = styled.main`
  width: 100vw;
  height: 100vh;
  position: relative;
`;

export const BackgroundImage = styled.div`
  /* background-image: url('https://images.pexels.com/photos/17368650/pexels-photo-17368650/free-photo-of-isuzu-car-among-rocks.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=2'); */
  background-image: url('https://images.pexels.com/photos/936250/pexels-photo-936250.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=2');
  background-repeat: no-repeat;
  background-size: cover;
  background-position: center;
  width: 100%;
  height: 100%;
  filter: blur(2px);
  z-index: 1;
  position: absolute;

  &::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-color: rgba(0, 0, 0, 0.5); /* Cor de sobreposição semitransparente */
    z-index: -1;
  }
`;

export const MainTitleContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;

  > h1, span {
    color: var(--white);
  }

  > h1 {
    font-size: 2.7rem;
  }

  > span {
    font-size: 1.1rem;
  }

  @media (min-width:768px) {
    > h1 {
      font-size: 8rem;
      font-family: 'Anton', sans-serif;
      letter-spacing: .3rem;
    }

    > span {
      font-size: 2rem;
      letter-spacing: .8rem;
      font-family: 'Poiret One', cursive;
    }
  }
`

export const TitleContainer = styled.section`
  z-index: 3;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 1rem;

  width: 100%;
  height: 100%;

  padding: 1rem;

  position: relative;

  > p {
    font-size: .5rem;
    font-style: italic;
    color: var(--white);
  }

  @media (min-width: 768px) {
    gap: 3.5rem;

    > p {
      font-size: 1rem;
    }
  }
`;

export const BrandsContainer = styled.div`
  display: flex;
  flex-direction: column;
  gap: 1rem;

  @media (min-width: 768px) {
    flex-direction: row;
    align-items: center;
  }
`

export const ListOurBrands = styled.ul`
  z-index: 3;
  display: flex;
  align-items: center;
  justify-content: center;

  position: relative;

  > li {
    display: flex;
    align-items: center;
    justify-content: center;

    color: var(--white);

    width: 35px;
    height: 35px;

    border-radius: 50%;
    border: .1px solid transparent;

    background-color: var(--gray);

    margin-right: -7px; /* Valor negativo para sobrepor as bordas */
    
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
  }

  @media (min-width: 768px) {
    > li {
      width: 50px;
      height: 50px;

      font-size: 1.7rem;
    }
  }
`

export const LoginBtn = styled(Link)`
  display: flex;
  align-items: center;
  gap: 1rem;

  padding: 1rem;

  border-radius: 20px;

  font-size: .8rem;
  font-weight: 600;

  color: var(--black);

  background: var(--white);

  transition: .4s ease;

  &:hover {
    justify-content: space-between;
    gap: 5rem;
    /* padding: 1rem 5.7rem; */

    color: var(--primary-color);
  }

  @media (min-width: 768px) {
    padding: 1rem 2rem;

    font-size: 1.1rem;
  }
`