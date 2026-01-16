import styled from "styled-components";

export const UserPageSection = styled.section`
  width: 100%;

  &:before {
    content: "";

    width: 100%;
    display: block;
    min-height: 300px;
    background: #4529e6;

    @media (max-width: 768px) {
      min-height: 308px;
    }
  }
`;

export const DivCar = styled.div`
  width: 85%;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  justify-content: center;
  max-width: 1400px;
  gap: 48px;
  padding-top: 64px;

  @media (max-width: 768px) {
    width: 85%;
    flex-direction: column;
    padding-left: 16px;
    padding-right: 16px;
  }
`;

export const DivCard = styled.div`
  width: 100%;
  max-width: 1240px;
  height: 327px;
  background-color: #ffffff;
  margin: 0 auto;
  padding: 15px;
  display: flex;
  flex-direction: column;
  gap: 15px;
  position: absolute;
  top: 35%;
  left: 54%;
  transform: translate(-55%, -50%);
  border-radius: 10px;
  margin-left: 3px;

  > div {
    width: 100px;
    height: 100px;
    background-color: #4529e6;
    border-radius: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
  }
`;

export const DivTest = styled.div`
  margin-top: 200px;
`;

export const ListCardUserPage = styled.ul`
  display: flex;
  justify-content: space-between;
  gap: 1rem;

  overflow-x: auto;

  padding: 1rem;

  @media (min-width: 768px) {
    flex-direction: row;
    flex-wrap: wrap;

    margin-top: 1.4rem;

    gap: 1.4rem;
  }
`;