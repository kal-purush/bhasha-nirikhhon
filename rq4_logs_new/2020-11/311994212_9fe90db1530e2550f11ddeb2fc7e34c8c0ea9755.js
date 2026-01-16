
import styled from "styled-components";

export const Wrapper = styled.div`
 margin: 0 5%;
@media (min-width: 576px) {
  margin: 0 10%;
  }
`;

export const ImageGridContainer = styled.div`
  margin: 0 20px;
  justify-content: center;
  display: flex;
  flex-wrap: wrap;
  padding: 0 4px;
  @media (min-width: 576px) {
    margin: 0;
  }
`;

export const ImageGridColumn = styled.div`
  flex: 100%;
  max-width: 100%;
  padding: 0 4px;
  background-color: #000;
  @media (min-width: 576px) {
    flex: 35%;
    max-width: 35%;
  }
  @media (min-width: 768px) {
    flex: 25%;
    max-width: 25%;
  }
   img {
  margin-top: 8px;
  vertical-align: middle;
  width: 100%;
  &:hover,
  &:focus {
    transform: scale3d(1.040, 1.040, 1);

    &::after {
      opacity: 1;
    }
  }
}
`
export const SectionTitle = styled.div`
  margin: ${({ isSmall }) => (isSmall ? "20px 0 " : "40px 0;")};
  text-align: center;
  font-family: 'Chivo', sans-serif;
  font-weight: 500;
  font-size: 1.5rem;
  letter-spacing: -0.03em;
  color: #344854;
:before,
:after {
  background-color: #000;
  content: "";
  display: inline-block;
  height: 2px;
  position: relative;
  vertical-align: middle;
  width: 10%;
}
:before {
  right: 0.5em;
  margin-left: -50%;
}
:after {
  left: 0.5em;
  margin-right: -50%;
}
@media (min-width: 576px) {
  margin: ${({ isSmall }) => (isSmall ? "60px 0 35px 0" : "60px 0;")};
  font-size: 2rem;
  }
  @media (min-width: 768px) {
    font-size: 2.5rem;
  }
`;

export const Subtitle = styled.div`
  margin-bottom: 40px;
  white-space: normal;
  text-align: center;
  font-family: 'Chivo', sans-serif;
  font-size: 1rem;
  color: #344854;
  a {
    color: #344854;

  }
  @media (min-width: 768px) {
    font-size: 1.125rem;
    margin-bottom: 70px;
  }
`;


