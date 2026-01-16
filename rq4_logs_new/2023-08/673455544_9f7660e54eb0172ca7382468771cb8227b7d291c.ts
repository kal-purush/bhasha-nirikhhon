import { styled } from "styled-components";

export const ModalBackgound = styled.div`
  width: 100%;
  height: 100vh;
  background-color: rgba(0, 0, 0, 0.5);
  position: fixed;
  top: 0;
  display: flex;
  justify-content: center;
  @media (min-width: 768px) {
    left: 0;
  }
`;

export const Modal = styled.div`
  width: 100%;
  height: 60vh;
  background-color: #fff;
  margin-top: 10%;
  border-radius: 15px;
  padding: 10px 20px;
  position: relative;
  display: block;

  .image-car {
    width: 95%;
    height: 75%;
    margin-top: 20px;

    @media (min-width: 768px) {
      width: 100%;
    }
  }

  @media (min-width: 768px) {
    width: 40%;
  }
`;

export const ModalTitle = styled.div`
  display: flex;
  justify-content: flex-start;
  align-items: center;

  h2 {
    margin: 20px 0;
    font-size: 16px;
    color: #212529;

    @media (min-width: 768px) {
      font-size: 20px;
    }
  }

  svg.close-btn-modalImage {
    position: absolute;
    right: 8%;
    font-size: 24px;
    cursor: pointer;
    color: #adb5bd;

    @media (min-width: 768px) {
      right: 3%;
    }
  }
`;